package com.tripleauth.hermetix.client.kis

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.OrderBookTick
import com.tripleauth.hermetix.broker.OrderEvent
import com.tripleauth.hermetix.broker.OrderEventType
import com.tripleauth.hermetix.broker.TradeTick
import com.tripleauth.hermetix.client.dto.OrderSide
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

    private fun newStream(htsId: String = "HTSUSER"): KisMarketStream =
        KisMarketStream(
            KisApiProperties(wsUrl = "ws://${server.hostName}:${server.port}/", htsId = htsId),
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


    @Test
    fun `호가 구독 - H0STASP0 프레임을 요청 표기 심볼의 호가창으로 전달한다`() {
        val books = LinkedBlockingQueue<OrderBookTick>()
        val s = newStream()
        s.subscribeOrderBook(listOf("KRX:005930")) { books.put(it) }
        s.connect()
        val conn = connections.take()
        val subscribe = objectMapper.readTree(conn.received.take())
        assertThat(subscribe.path("body").path("input").path("tr_id").asText()).isEqualTo("H0STASP0")

        conn.socket.send("0|H0STASP0|001|$BOOK_FIELDS")
        val book = books.take()
        assertThat(book.symbol).isEqualTo("KRX:005930")
        assertThat(book.asks).hasSize(10)
        assertThat(book.bestAsk?.price).isEqualByComparingTo("250500")
        assertThat(book.bestAsk?.quantity).isEqualByComparingTo("1000")
        assertThat(book.bestBid?.price).isEqualByComparingTo("250000")
        assertThat(book.bestBid?.quantity).isEqualByComparingTo("2000")
        assertThat(book.asks[9].price).isEqualByComparingTo("255000")
        assertThat(book.totalAskQuantity).isEqualByComparingTo("55000")
        assertThat(book.totalBidQuantity).isEqualByComparingTo("65000")
    }

    @Test
    fun `주문 통보 - 구독 응답의 key·iv 로 암호화 프레임을 복호화해 이벤트로 전달한다`() {
        val events = LinkedBlockingQueue<OrderEvent>()
        val s = newStream()
        s.subscribeOrderEvents { events.put(it) }
        s.connect()
        val conn = connections.take()
        val subscribe = objectMapper.readTree(conn.received.take())
        assertThat(subscribe.path("body").path("input").path("tr_id").asText()).isEqualTo("H0STCNI9") // 모의
        assertThat(subscribe.path("body").path("input").path("tr_key").asText()).isEqualTo("HTSUSER")

        conn.socket.send("""{"header":{"tr_id":"H0STCNI9","tr_key":"HTSUSER","encrypt":"Y"},"body":{"rt_cd":"0","msg_cd":"OPSP0000","msg1":"SUBSCRIBE SUCCESS","output":{"iv":"$IV","key":"$KEY"}}}""")
        val accepted = "HTSUSER^50199202^0000012345^0000000000^02^0^00^0^005930^0^0^105530^0^1^1^00950^3^홍길동^0^^N^^^^삼성전자^250000"
        val filled = "HTSUSER^50199202^0000012345^0000000000^02^0^00^0^005930^3^250000^105531^0^2^2^00950^3^홍길동^0^^N^^^^삼성전자^250000"
        conn.socket.send("1|H0STCNI9|001|" + KisMarketStream.encrypt(accepted, KEY, IV))
        conn.socket.send("1|H0STCNI9|001|" + KisMarketStream.encrypt(filled, KEY, IV))

        val e1 = events.take()
        assertThat(e1.type).isEqualTo(OrderEventType.ACCEPTED)
        assertThat(e1.orderId).isEqualTo("0000012345")
        assertThat(e1.orderIdMatches("12345")).isTrue()
        assertThat(e1.symbol).isEqualTo("005930")
        assertThat(e1.side).isEqualTo(OrderSide.BUY)
        assertThat(e1.quantity).isEqualByComparingTo("3")
        assertThat(e1.price).isEqualByComparingTo("250000")

        val e2 = events.take()
        assertThat(e2.type).isEqualTo(OrderEventType.FILLED)
        assertThat(e2.quantity).isEqualByComparingTo("3")
        assertThat(e2.price).isEqualByComparingTo("250000")
    }

    @Test
    fun `주문 통보 파싱 - 취소·거부 구분`() {
        val today = LocalDate.of(2026, 9, 14)
        val canceled = "U^A^0000000002^0000000001^01^2^00^0^005930^0^0^105530^0^1^2^00950^3^N^0^^N^^^^S^0"
        val rejected = "U^A^0000000003^0000000000^02^0^00^0^005930^0^0^105530^1^1^1^00950^3^N^0^^N^^^^S^0"
        val c = KisMarketStream.parseOrderEventFrame("0|H0STCNI9|001|$canceled", today).single()
        assertThat(c.type).isEqualTo(OrderEventType.CANCELED)
        assertThat(c.side).isEqualTo(OrderSide.SELL)
        assertThat(c.originalOrderId).isEqualTo("0000000001")
        val r = KisMarketStream.parseOrderEventFrame("0|H0STCNI0|001|$rejected", today).single()
        assertThat(r.type).isEqualTo(OrderEventType.REJECTED)
        assertThat(KisMarketStream.parseOrderEventFrame("0|H0STCNT0|001|$canceled", today)).isEmpty()
    }

    @Test
    fun `HTS ID 없이 주문 통보를 구독하면 실패한다`() {
        val s = newStream(htsId = "")
        org.junit.jupiter.api.assertThrows<IllegalStateException> { s.subscribeOrderEvents { } }
    }

    @Test
    fun `AES 복호화는 암호화의 역이다`() {
        val plain = "005930^105530^250000"
        assertThat(KisMarketStream.decrypt(KisMarketStream.encrypt(plain, KEY, IV), KEY, IV)).isEqualTo(plain)
    }

    companion object {
        /** H0STCNT0 본문 20필드 — 0 코드, 1 시각, 2 현재가, 3 부호, 4 대비, 5 대비율, 6 가중평균, 7 시가, 8 고가, 9 저가, 10 매도1, 11 매수1, 12 체결량, 13 누적량 … */
        const val FRAME_FIELDS = "005930^093012^71500^5^300^-0.42^71480^71800^71900^71300^71500^71400^15^1234567^88000000000^1200^1300^100^105.2^600000"

        /** H0STASP0 본문 — 0 코드, 1 시각, 2 시간구분, 3-12 매도호가, 13-22 매수호가, 23-32 매도잔량, 33-42 매수잔량, 43 총매도잔량, 44 총매수잔량 */
        val BOOK_FIELDS: String = listOf(
            "005930", "105530", "0",
            *(0 until 10).map { (250500 + 500 * it).toString() }.toTypedArray(),
            *(0 until 10).map { (250000 - 500 * it).toString() }.toTypedArray(),
            *(0 until 10).map { (1000 * (it + 1)).toString() }.toTypedArray(),
            *(0 until 10).map { (2000 * (it + 1)).toString() }.toTypedArray(),
            "55000", "65000", "0", "0",
        ).joinToString("^")

        const val KEY = "zkkljxnqkyodprlmaksyyilhzjmxqjer" // 32자 (실측 구독 응답과 같은 형식)
        const val IV = "d82e2f422913e3b2" // 16자
    }
}
