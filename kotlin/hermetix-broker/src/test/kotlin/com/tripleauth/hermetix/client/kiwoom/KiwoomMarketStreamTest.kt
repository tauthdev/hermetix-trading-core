package com.tripleauth.hermetix.client.kiwoom

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
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class KiwoomMarketStreamTest {

    private val objectMapper = ObjectMapper().registerModule(kotlinModule())
    private lateinit var server: MockWebServer
    private var stream: KiwoomMarketStream? = null

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

    private fun newStream(): KiwoomMarketStream =
        KiwoomMarketStream(
            KiwoomApiProperties(wsUrl = "ws://${server.hostName}:${server.port}/api/dostk/websocket"),
            objectMapper,
            token = { "ACCESS-TOKEN" },
        ).also { stream = it }

    private fun <T> LinkedBlockingQueue<T>.take(seconds: Long = 5): T =
        poll(seconds, TimeUnit.SECONDS) ?: error("${seconds}s 안에 도착하지 않음")

    @Test
    fun `접속 → LOGIN → REG 순서로 진행하고 REAL 프레임을 틱으로 전달한다`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val s = newStream()
        s.subscribeTrades(listOf("KRX:005930")) { ticks.put(it) }
        s.connect()

        val conn = connections.take()
        val login = objectMapper.readTree(conn.received.take())
        assertThat(login.path("trnm").asText()).isEqualTo("LOGIN")
        assertThat(login.path("token").asText()).isEqualTo("ACCESS-TOKEN")
        assertThat(s.isConnected).isFalse() // 로그인 응답 전

        conn.socket.send("""{"trnm":"LOGIN","return_code":0,"return_msg":""}""")
        val reg = objectMapper.readTree(conn.received.take())
        assertThat(reg.path("trnm").asText()).isEqualTo("REG")
        assertThat(reg.path("data")[0].path("item")[0].asText()).isEqualTo("005930")
        assertThat(reg.path("data")[0].path("type")[0].asText()).isEqualTo("0B")

        conn.socket.send("""{"trnm":"REG","return_code":0,"return_msg":""}""")
        conn.socket.send(REAL_FRAME)

        val tick = ticks.take()
        assertThat(tick.symbol).isEqualTo("KRX:005930")
        assertThat(tick.price).isEqualByComparingTo("71500")
        assertThat(tick.quantity).isEqualByComparingTo("15")
        assertThat(s.isConnected).isTrue()
    }

    @Test
    fun `PING 은 받은 그대로 돌려보낸다`() {
        val s = newStream()
        s.connect()
        val conn = connections.take()
        conn.received.take() // LOGIN
        val ping = """{"trnm":"PING"}"""
        conn.socket.send(ping)
        assertThat(conn.received.take()).isEqualTo(ping)
    }

    @Test
    fun `서버가 끊으면 재접속해 다시 로그인하고 등록한다`() {
        val s = newStream()
        s.subscribeTrades(listOf("005930")) { }
        s.connect()
        val first = connections.take()
        first.received.take() // LOGIN
        first.socket.close(1000, "bye")

        val second = connections.take(10)
        assertThat(objectMapper.readTree(second.received.take()).path("trnm").asText()).isEqualTo("LOGIN")
        second.socket.send("""{"trnm":"LOGIN","return_code":0}""")
        assertThat(objectMapper.readTree(second.received.take()).path("trnm").asText()).isEqualTo("REG")
    }

    @Test
    fun `REAL 파싱 - 부호 제거, A 프리픽스 제거, 0B 외 타입 무시`() {
        val today = LocalDate.of(2026, 9, 14)
        val tick = KiwoomMarketStream.parseReal(objectMapper.readTree(REAL_FRAME), today).single()
        assertThat(tick.symbol).isEqualTo("005930")
        assertThat(tick.price).isEqualByComparingTo("71500")
        assertThat(tick.askPrice).isEqualByComparingTo("71500")
        assertThat(tick.bidPrice).isEqualByComparingTo("71400")
        assertThat(tick.quantity).isEqualByComparingTo("15")
        assertThat(tick.cumulativeVolume).isEqualTo(1234567L)
        assertThat(tick.change).isEqualByComparingTo("-300")
        assertThat(tick.changeRate).isEqualByComparingTo("-0.0042")
        assertThat(tick.timestamp.atZone(ZoneId.of("Asia/Seoul")).toLocalTime().toString()).isEqualTo("09:30:12")

        val orderbook = """{"trnm":"REAL","data":[{"type":"0D","name":"주식호가잔량","item":"005930","values":{"21":"093012"}}]}"""
        assertThat(KiwoomMarketStream.parseReal(objectMapper.readTree(orderbook), today)).isEmpty()
    }


    @Test
    fun `호가·주문체결 등록 - 0D 는 종목으로, 00 은 빈 item 으로 REG 하고 REAL 을 각 리스너에 전달한다`() {
        val books = LinkedBlockingQueue<OrderBookTick>()
        val events = LinkedBlockingQueue<OrderEvent>()
        val s = newStream()
        s.subscribeOrderBook(listOf("KRX:005930")) { books.put(it) }
        s.subscribeOrderEvents { events.put(it) }
        s.connect()
        val conn = connections.take()
        conn.received.take() // LOGIN
        conn.socket.send("""{"trnm":"LOGIN","return_code":0}""")
        val regs = (1..2).map { objectMapper.readTree(conn.received.take()) }
        val bookReg = regs.first { it.path("data")[0].path("type")[0].asText() == "0D" }
        assertThat(bookReg.path("data")[0].path("item")[0].asText()).isEqualTo("005930")
        val orderReg = regs.first { it.path("data")[0].path("type")[0].asText() == "00" }
        assertThat(orderReg.path("data")[0].path("item")[0].asText()).isEqualTo("")

        conn.socket.send(BOOK_FRAME)
        val book = books.take()
        assertThat(book.symbol).isEqualTo("KRX:005930")
        assertThat(book.bestAsk?.price).isEqualByComparingTo("250500")
        assertThat(book.bestAsk?.quantity).isEqualByComparingTo("1000")
        assertThat(book.bestBid?.price).isEqualByComparingTo("250000")
        assertThat(book.bids[1].price).isEqualByComparingTo("249500")
        assertThat(book.totalAskQuantity).isEqualByComparingTo("55000")

        conn.socket.send(ORDER_FRAME)
        val e = events.take()
        assertThat(e.type).isEqualTo(OrderEventType.FILLED)
        assertThat(e.orderId).isEqualTo("0000012345")
        assertThat(e.symbol).isEqualTo("005930")
        assertThat(e.side).isEqualTo(OrderSide.BUY)
        assertThat(e.quantity).isEqualByComparingTo("1")
        assertThat(e.price).isEqualByComparingTo("250000")
        assertThat(e.remainingQuantity).isEqualByComparingTo("0")
    }

    @Test
    fun `주문체결 파싱 - 접수·취소·거부`() {
        val today = LocalDate.of(2026, 9, 14)
        fun frame(status: String, kind: String, filledQty: String = "0", reason: String = "") = objectMapper.readTree(
            """{"trnm":"REAL","data":[{"type":"00","name":"주문체결","item":"A005930","values":{"9203":"0000012346","904":"0000000000","9001":"A005930","913":"$status","905":"$kind","907":"2","900":"1","901":"+240000","902":"1","910":"","911":"$filledQty","908":"105530","919":"$reason"}}]}""",
        )
        val accepted = KiwoomMarketStream.parseOrderEvents(frame("접수", "+매수"), today).single()
        assertThat(accepted.type).isEqualTo(OrderEventType.ACCEPTED)
        assertThat(accepted.quantity).isEqualByComparingTo("1")
        assertThat(accepted.price).isEqualByComparingTo("240000")
        assertThat(KiwoomMarketStream.parseOrderEvents(frame("확인", "매수취소"), today).single().type).isEqualTo(OrderEventType.CANCELED)
        assertThat(KiwoomMarketStream.parseOrderEvents(frame("확인", "매수정정"), today).single().type).isEqualTo(OrderEventType.MODIFIED)
        val rejected = KiwoomMarketStream.parseOrderEvents(frame("거부", "+매수", reason = "주문가능금액 부족"), today).single()
        assertThat(rejected.type).isEqualTo(OrderEventType.REJECTED)
        assertThat(rejected.reason).isEqualTo("주문가능금액 부족")
    }

    companion object {
        /** 0D 주식호가잔량 — 41-50 매도호가, 61-70 매도잔량, 51-60 매수호가, 71-80 매수잔량 (가격에 등락 부호) */
        val BOOK_FRAME: String = run {
            val v = mutableMapOf("21" to "105530", "121" to "55000", "125" to "65000")
            for (i in 0 until 10) {
                v["${41 + i}"] = "-${250500 + 500 * i}"; v["${61 + i}"] = "${1000 * (i + 1)}"
                v["${51 + i}"] = "-${250000 - 500 * i}"; v["${71 + i}"] = "${2000 * (i + 1)}"
            }
            val values = v.entries.joinToString(",") { "\"${it.key}\":\"${it.value}\"" }
            """{"data":[{"values":{$values},"type":"0D","name":"주식호가잔량","item":"A005930"}],"trnm":"REAL"}"""
        }
        const val ORDER_FRAME = """{"data":[{"values":{"9203":"0000012345","904":"0000000000","9001":"A005930","913":"체결","905":"+매수","907":"2","900":"1","901":"+250000","902":"0","910":"+250000","911":"1","908":"105531","919":""},"type":"00","name":"주문체결","item":""}],"trnm":"REAL"}"""
        const val REAL_FRAME = """{"trnm":"REAL","data":[{"type":"0B","name":"주식체결","item":"A005930","values":{"20":"093012","10":"-71500","11":"-300","12":"-0.42","27":"+71500","28":"+71400","15":"-15","13":"1234567","14":"88000000000","16":"71800","17":"71900","18":"71300"}}]}"""
    }
}
