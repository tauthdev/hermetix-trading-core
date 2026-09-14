package com.tripleauth.hermetix.client.kis

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.TradeTick
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class KisMarketStreamTest {

    private val objectMapper = ObjectMapper().registerModule(kotlinModule())
    private lateinit var server: MockWebServer
    private var stream: KisMarketStream? = null

    /** 서버 쪽에서 본 연결 하나 — 받은 메시지 큐와 보낼 수 있는 소켓 */
    private class ServerConnection(val socket: WebSocket) {
        val received = LinkedBlockingQueue<String>()
    }

    private val connections = LinkedBlockingQueue<ServerConnection>()
    private val allConnections = java.util.concurrent.CopyOnWriteArrayList<ServerConnection>()

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse =
                MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                    lateinit var conn: ServerConnection
                    override fun onOpen(webSocket: WebSocket, response: Response) {
                        conn = ServerConnection(webSocket).also { allConnections += it; connections.put(it) }
                    }
                    override fun onMessage(webSocket: WebSocket, text: String) {
                        conn.received.put(text)
                    }
                    // OkHttp 서버 소켓은 close 프레임을 받아도 앱이 close() 로 응답해야 핸드셰이크가 끝난다 — 안 하면 shutdown 이 5초 대기
                    override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                        webSocket.close(code, reason)
                    }
                })
        }
        server.start()
    }

    @AfterEach
    fun tearDown() {
        stream?.close()
        allConnections.forEach { runCatching { it.socket.close(1000, null) } }
        server.shutdown()
    }

    private fun newStream(): KisMarketStream =
        KisMarketStream(
            KisApiProperties(wsUrl = "ws://${server.hostName}:${server.port}/"),
            objectMapper,
            approvalKey = { "APPROVAL-KEY" },
        ).also { stream = it }

    private fun <T> LinkedBlockingQueue<T>.take(seconds: Long = 5): T =
        poll(seconds, TimeUnit.SECONDS) ?: error("${seconds}s 안에 도착하지 않음")

    @Test
    fun `접속하면 승인키를 실은 구독 메시지를 보내고, 데이터 프레임을 요청 표기 심볼의 틱으로 전달한다`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val s = newStream()
        s.subscribeTrades(listOf("KRX:005930")) { ticks.put(it) }
        s.connect()

        val conn = connections.take()
        val subscribe = objectMapper.readTree(conn.received.take())
        assertThat(subscribe.path("header").path("approval_key").asText()).isEqualTo("APPROVAL-KEY")
        assertThat(subscribe.path("header").path("tr_type").asText()).isEqualTo("1")
        assertThat(subscribe.path("body").path("input").path("tr_id").asText()).isEqualTo("H0STCNT0")
        assertThat(subscribe.path("body").path("input").path("tr_key").asText()).isEqualTo("005930")

        conn.socket.send("""{"header":{"tr_id":"H0STCNT0","tr_key":"005930","encrypt":"N"},"body":{"rt_cd":"0","msg_cd":"OPSP0000","msg1":"SUBSCRIBE SUCCESS"}}""")
        conn.socket.send("0|H0STCNT0|001|" + FRAME_FIELDS)

        val tick = ticks.take()
        assertThat(tick.symbol).isEqualTo("KRX:005930")
        assertThat(tick.price).isEqualByComparingTo("71500")
        assertThat(tick.quantity).isEqualByComparingTo("15")
        assertThat(tick.cumulativeVolume).isEqualTo(1234567L)
        assertThat(s.isConnected).isTrue()
    }

    @Test
    fun `PINGPONG 은 받은 그대로 돌려보낸다`() {
        val s = newStream()
        s.connect()
        val conn = connections.take()
        val ping = """{"header":{"tr_id":"PINGPONG","datetime":"20260914093000"}}"""
        conn.socket.send(ping)
        assertThat(conn.received.take()).isEqualTo(ping)
    }

    @Test
    fun `서버가 끊으면 재접속하고 구독을 다시 보낸다`() {
        val s = newStream()
        s.subscribeTrades(listOf("005930")) { }
        s.connect()
        val first = connections.take()
        first.received.take() // 첫 구독
        first.socket.close(1000, "server going away")

        val second = connections.take(10)
        val resubscribe = objectMapper.readTree(second.received.take())
        assertThat(resubscribe.path("body").path("input").path("tr_key").asText()).isEqualTo("005930")
    }

    @Test
    fun `연결된 뒤 추가 구독은 즉시 전송된다`() {
        val s = newStream()
        s.connect()
        val conn = connections.take()
        s.subscribeTrades(listOf("000660")) { }
        val subscribe = objectMapper.readTree(conn.received.take())
        assertThat(subscribe.path("body").path("input").path("tr_key").asText()).isEqualTo("000660")
    }

    @Test
    fun `프레임 파싱 - 필드 인덱스와 부호, 여러 레코드`() {
        val today = LocalDate.of(2026, 9, 14)
        val one = KisMarketStream.parseFrame("0|H0STCNT0|001|$FRAME_FIELDS", today).single()
        assertThat(one.symbol).isEqualTo("005930")
        assertThat(one.price).isEqualByComparingTo("71500")
        assertThat(one.askPrice).isEqualByComparingTo("71500")
        assertThat(one.bidPrice).isEqualByComparingTo("71400")
        assertThat(one.quantity).isEqualByComparingTo("15")
        assertThat(one.cumulativeVolume).isEqualTo(1234567L)
        assertThat(one.change).isEqualByComparingTo("-300") // 부호코드 5(하락) + 무부호 값 → 음수
        assertThat(one.changeRate).isEqualByComparingTo(BigDecimal("-0.0042"))
        assertThat(one.timestamp.atZone(ZoneId.of("Asia/Seoul")).toLocalTime().toString()).isEqualTo("09:30:12")

        val two = KisMarketStream.parseFrame("0|H0STCNT0|002|$FRAME_FIELDS^$FRAME_FIELDS", today)
        assertThat(two).hasSize(2)

        assertThat(KisMarketStream.parseFrame("0|H0STASP0|001|$FRAME_FIELDS", today)).isEmpty() // 다른 TR
        assertThat(KisMarketStream.parseFrame("0|H0STCNT0|001|005930^093012", today)).isEmpty() // 필드 부족
    }

    companion object {
        /** H0STCNT0 본문 20필드 — 0 코드, 1 시각, 2 현재가, 3 부호, 4 대비, 5 대비율, 6 가중평균, 7 시가, 8 고가, 9 저가, 10 매도1, 11 매수1, 12 체결량, 13 누적량 … */
        const val FRAME_FIELDS = "005930^093012^71500^5^300^-0.42^71480^71800^71900^71300^71500^71400^15^1234567^88000000000^1200^1300^100^105.2^600000"
    }
}
