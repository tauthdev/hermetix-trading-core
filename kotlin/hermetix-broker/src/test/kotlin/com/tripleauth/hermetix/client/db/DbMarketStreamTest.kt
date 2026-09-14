package com.tripleauth.hermetix.client.db

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.KrxCalendar
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
import java.io.File
import java.time.LocalDate
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/** DB증권 웹소켓 — 문서 기반 프레임(픽스처 `db.json#stream`)으로 구독 메시지·라우팅·파서를 검증한다 (실측 전) */
class DbMarketStreamTest {

    private val objectMapper = ObjectMapper().registerModule(kotlinModule())
    private lateinit var server: MockWebServer
    private var stream: DbMarketStream? = null
    private val today: LocalDate = LocalDate.of(2026, 9, 14)

    private class ServerConnection(val socket: WebSocket) {
        val received = LinkedBlockingQueue<String>()
    }

    private val connections = LinkedBlockingQueue<ServerConnection>()
    private val allConnections = CopyOnWriteArrayList<ServerConnection>()

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

    private fun newStream(): DbMarketStream =
        DbMarketStream(DbApiProperties(wsUrl = "ws://${server.hostName}:${server.port}/websocket"), objectMapper, token = { "ACCESS-TOKEN" })
            .also { stream = it }

    private fun <T> LinkedBlockingQueue<T>.take(seconds: Long = 5): T =
        poll(seconds, TimeUnit.SECONDS) ?: error("${seconds}s 안에 도착하지 않음")

    private fun fixture(): JsonNode {
        val file = File("../../conformance/fixtures/db.json")
        check(file.exists()) { "픽스처가 없다: ${file.absolutePath}" }
        return objectMapper.readTree(file).path("stream")
    }

    private fun frame(node: JsonNode): String = node.asText()

    @Test
    fun `체결 구독 - 토큰 헤더·tr_type 1·tr_key 'J '+코드, S00 프레임을 요청 표기 심볼의 틱으로 전달한다`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val s = newStream()
        s.subscribeTrades(listOf("KRX:005930")) { ticks.put(it) }
        s.connect()

        val conn = connections.take()
        val subscribe = objectMapper.readTree(conn.received.take())
        assertThat(subscribe.path("header").path("token").asText()).isEqualTo("ACCESS-TOKEN")
        assertThat(subscribe.path("header").path("tr_type").asText()).isEqualTo("1")
        assertThat(subscribe.path("body").path("tr_cd").asText()).isEqualTo("S00")
        assertThat(subscribe.path("body").path("tr_key").asText()).isEqualTo("J 005930")

        conn.socket.send("""{"header":{"tr_cd":"S00","tr_key":null},"body":{"tr_key":["J 005930"]}}""") // ack
        conn.socket.send("""{"header":null,"body":null}""") // keepalive 류 — 무시
        conn.socket.send(frame(fixture()["frames"][0]))

        val tick = ticks.take()
        assertThat(tick.symbol).isEqualTo("KRX:005930") // U-005930 → 005930 → 요청 표기
        assertThat(tick.price).isEqualByComparingTo("143300")
        assertThat(tick.quantity).isEqualByComparingTo("100")
        assertThat(tick.cumulativeVolume).isEqualTo(405039L)
        assertThat(tick.askPrice).isEqualByComparingTo("140200")
        assertThat(tick.bidPrice).isEqualByComparingTo("143300")
        assertThat(tick.change).isEqualByComparingTo("33000")
        assertThat(tick.changeRate).isEqualByComparingTo("0.2992")
        assertThat(s.isConnected).isTrue()
    }

    @Test
    fun `호가·주문 통보 - S01 은 tr_type 1, IS0·IS1 은 tr_type 3 으로 tr_key 없이 등록한다`() {
        val books = LinkedBlockingQueue<OrderBookTick>()
        val events = LinkedBlockingQueue<OrderEvent>()
        val s = newStream()
        s.subscribeOrderBook(listOf("005930")) { books.put(it) }
        s.subscribeOrderEvents { events.put(it) }
        s.connect()

        val conn = connections.take()
        val msgs = (1..3).map { objectMapper.readTree(conn.received.take()) }
        val book = msgs.first { it.path("body").path("tr_cd").asText() == "S01" }
        assertThat(book.path("header").path("tr_type").asText()).isEqualTo("1")
        assertThat(book.path("body").path("tr_key").asText()).isEqualTo("J 005930")
        listOf("IS0", "IS1").forEach { tr ->
            val reg = msgs.first { it.path("body").path("tr_cd").asText() == tr }
            assertThat(reg.path("header").path("tr_type").asText()).isEqualTo("3")
            assertThat(reg.path("body").has("tr_key")).isFalse()
        }

        val fx = fixture()
        conn.socket.send(frame(fx["orderBook"]["frames"][0]))
        val b = books.take()
        assertThat(b.symbol).isEqualTo("005930")
        assertThat(b.bestAsk?.price).isEqualByComparingTo("54500")
        assertThat(b.bestAsk?.quantity).isEqualByComparingTo("1000")
        assertThat(b.bestBid?.price).isEqualByComparingTo("54400")
        assertThat(b.asks).hasSize(10)
        assertThat(b.totalAskQuantity).isEqualByComparingTo("809630")

        fx["orderEvents"]["frames"].forEach { conn.socket.send(frame(it)) }
        val accepted = events.take()
        assertThat(accepted.type).isEqualTo(OrderEventType.ACCEPTED)
        assertThat(accepted.orderId).isEqualTo("0000034048")
        assertThat(accepted.orderIdMatches("34048")).isTrue()
        assertThat(accepted.symbol).isEqualTo("005930")
        assertThat(accepted.side).isEqualTo(OrderSide.BUY)
        assertThat(accepted.quantity).isEqualByComparingTo("10")
        assertThat(accepted.price).isEqualByComparingTo("80000")
        val filled = events.take()
        assertThat(filled.type).isEqualTo(OrderEventType.FILLED)
        assertThat(filled.quantity).isEqualByComparingTo("4")
        assertThat(filled.remainingQuantity).isEqualByComparingTo("6")
        val canceled = events.take()
        assertThat(canceled.type).isEqualTo(OrderEventType.CANCELED)
        assertThat(canceled.orderId).isEqualTo("0000241048")
        assertThat(canceled.originalOrderId).isEqualTo("0000241038")
        assertThat(canceled.symbol).isEqualTo("004410")
    }

    @Test
    fun `서버가 끊으면 재접속해 구독과 계좌 등록을 다시 보낸다`() {
        val s = newStream()
        s.subscribeTrades(listOf("005930")) { }
        s.subscribeOrderEvents { }
        s.connect()
        val first = connections.take()
        repeat(3) { first.received.take() }
        first.socket.close(1000, "bye")

        val second = connections.take(10)
        val resent = (1..3).map { objectMapper.readTree(second.received.take()).path("body").path("tr_cd").asText() }
        assertThat(resent).containsExactlyInAnyOrder("S00", "IS0", "IS1")
    }

    @Test
    fun `제어 프레임 - rsp_cd 가 header 또는 body 에 있으면 데이터로 다루지 않는다`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val s = newStream()
        s.subscribeTrades(listOf("005930")) { ticks.put(it) }
        s.connect()
        val conn = connections.take()
        conn.received.take()
        conn.socket.send("""{"header":{"tr_cd":"S00","rsp_cd":"10017","rsp_msg":"종목코드가 없습니다"},"body":null}""")
        conn.socket.send("""{"header":null,"body":{"rsp_cd":"00000","rsp_msg":"정상"}}""")
        assertThat(ticks.poll(1, TimeUnit.SECONDS)).isNull()
        assertThat(s.isConnected).isTrue()
    }

    @Test
    fun `파서 - 필드명 대소문자 무시, 하락 부호, 0 호가 제외, 거부·정정 판정`() {
        val lower = objectMapper.readTree("""{"shrniscd":"N-005930","stckcntghour":"93012","stckprpr":"71500","cntgvol":"15","acmlvol":"1234567","prdyvrss":"300","prdyvrssclr":"-","prdyctrt":"-0.42","askp1":"71500","bidp1":"71400"}""")
        val t = DbMarketStream.parseTrade(lower, today)!!
        assertThat(t.symbol).isEqualTo("005930")
        assertThat(t.change).isEqualByComparingTo("-300")
        assertThat(t.changeRate).isEqualByComparingTo("-0.0042")
        assertThat(t.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo("09:30:12")

        val sparse = objectMapper.readTree("""{"ShrnIscd":"005930","BsopHour":"100647","Askp1":"54500","AskpRsqn1":"10","Askp2":"0","Bidp1":"54400","BidpRsqn1":"20","TotalAskprsqn":"10","TotalBidprsqn":"20"}""")
        val b = DbMarketStream.parseOrderBook(sparse, today)!!
        assertThat(b.asks).hasSize(1)
        assertThat(b.bids).hasSize(1)

        val rejected = objectMapper.readTree("""{"Sordno":"0000000007","Sorgordno":"0000000000","Sshtnisuno":"A005930","Sbnstp":"1","Sordqty":"0000000000000002","Sordprc":"0000000070000","Srjtqty":"0000000000000002","Sexecqty":"0000000000000000","Scanccnfqty":"0000000000000000","Smdfycnfqty":"0000000000000000","Sunercqty":"0000000000000000","Sexectime":"101020317"}""")
        val r = DbMarketStream.parseOrderEvent("IS1", rejected, today)!!
        assertThat(r.type).isEqualTo(OrderEventType.REJECTED)
        assertThat(r.side).isEqualTo(OrderSide.SELL)
        assertThat(r.quantity).isEqualByComparingTo("2")
        assertThat(r.originalOrderId).isNull()

        val modified = objectMapper.readTree("""{"Sordno":"0000000009","Sorgordno":"0000000007","Sshtnisuno":"A005930","Sbnstp":"2","Sordqty":"0000000000000002","Sordprc":"0000000071000","Srjtqty":"0000000000000000","Sexecqty":"0000000000000000","Scanccnfqty":"0000000000000000","Smdfycnfqty":"0000000000000002","Smdfycnfprc":"0000000000071000","Sunercqty":"0000000000000002","Sexectime":"101020317"}""")
        val m = DbMarketStream.parseOrderEvent("IS1", modified, today)!!
        assertThat(m.type).isEqualTo(OrderEventType.MODIFIED)
        assertThat(m.price).isEqualByComparingTo("71000")
        assertThat(m.originalOrderId).isEqualTo("0000000007")

        assertThat(DbMarketStream.parseOrderEvent("IS0", objectMapper.readTree("""{"Sshtnisuno":"A005930"}"""), today)).isNull() // 주문번호 없음
    }

    @Test
    fun `픽스처 - stream 섹션의 문서 기반 프레임을 기대값대로 파싱한다`() {
        val fx = fixture()
        assertThat(fx["measured"].asBoolean()).isFalse()

        val trades = fx["frames"].map { DbMarketStream.parseTrade(objectMapper.readTree(it.asText()).path("body"), today)!! }
        fx["expected"].zip(trades).forEach { (e, t) ->
            assertThat(t.symbol).isEqualTo(e["symbol"].asText())
            assertThat(t.price).isEqualByComparingTo(e["price"].asText())
            assertThat(t.quantity).isEqualByComparingTo(e["quantity"].asText())
            assertThat(t.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
            assertThat(t.askPrice).isEqualByComparingTo(e["askPrice"].asText())
            assertThat(t.bidPrice).isEqualByComparingTo(e["bidPrice"].asText())
            assertThat(t.cumulativeVolume).isEqualTo(e["cumulativeVolume"].asLong())
            assertThat(t.change).isEqualByComparingTo(e["change"].asText())
            assertThat(t.changeRate).isEqualByComparingTo(e["changeRate"].asText())
        }

        val books = fx["orderBook"]["frames"].map { DbMarketStream.parseOrderBook(objectMapper.readTree(it.asText()).path("body"), today)!! }
        fx["orderBook"]["expected"].zip(books).forEach { (e, b) ->
            assertThat(b.symbol).isEqualTo(e["symbol"].asText())
            assertThat(b.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
            fun levels(n: JsonNode) = n.map { it["price"].asText() to it["quantity"].asText() }
            assertThat(b.asks.map { it.price.toPlainString() to it.quantity.toPlainString() }).isEqualTo(levels(e["asks"]))
            assertThat(b.bids.map { it.price.toPlainString() to it.quantity.toPlainString() }).isEqualTo(levels(e["bids"]))
            assertThat(b.totalAskQuantity).isEqualByComparingTo(e["totalAskQuantity"].asText())
            assertThat(b.totalBidQuantity).isEqualByComparingTo(e["totalBidQuantity"].asText())
        }

        val events = fx["orderEvents"]["frames"].map { raw ->
            val node = objectMapper.readTree(raw.asText())
            DbMarketStream.parseOrderEvent(node.path("header").path("tr_cd").asText(), node.path("body"), today)!!
        }
        fx["orderEvents"]["expected"].zip(events).forEach { (e, ev) ->
            assertThat(ev.orderId).isEqualTo(e["orderId"].asText())
            assertThat(ev.type.name).isEqualTo(e["type"].asText())
            assertThat(ev.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
            assertThat(ev.symbol).isEqualTo(e["symbol"].asText())
            assertThat(ev.side?.name).isEqualTo(e["side"].asText())
            assertThat(ev.quantity).isEqualByComparingTo(e["quantity"].asText())
            assertThat(ev.price).isEqualByComparingTo(e["price"].asText())
            if (e.has("remainingQuantity")) assertThat(ev.remainingQuantity).isEqualByComparingTo(e["remainingQuantity"].asText())
            if (e.has("originalOrderId")) assertThat(ev.originalOrderId).isEqualTo(e["originalOrderId"].asText()) else assertThat(ev.originalOrderId).isNull()
        }
    }
}
