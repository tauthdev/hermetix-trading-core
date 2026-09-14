package com.tripleauth.hermetix.client.toss

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
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/** 토스 웹소켓 — 선언형 구독·PING 하트비트·프레임 파싱 (AsyncAPI 1.2.2 문서 기반, 실측 전) */
class TossMarketStreamTest {

    private val objectMapper = ObjectMapper().registerModule(kotlinModule())
    private lateinit var server: MockWebServer
    private var stream: TossMarketStream? = null

    private class ServerConnection(val socket: WebSocket, val request: RecordedRequest) {
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
                        conn = ServerConnection(webSocket, request).also { allConnections += it; connections.put(it) }
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

    private fun newStream(heartbeatMillis: Long = 0, accountSeq: String = "3"): TossMarketStream =
        TossMarketStream(
            TossApiProperties(wsUrl = "ws://${server.hostName}:${server.port}/ws/v1"),
            objectMapper,
            token = { "ACCESS-TOKEN" },
            accountSeq = { accountSeq },
            heartbeatMillis = heartbeatMillis,
            declareDelayMillis = 50,
        ).also { stream = it }

    private fun <T> LinkedBlockingQueue<T>.take(seconds: Long = 5): T =
        poll(seconds, TimeUnit.SECONDS) ?: error("${seconds}s 안에 도착하지 않음")

    /** 다음 선언(배열) 프레임을 읽어 type → codes 로 정리 */
    private fun nextDeclaration(conn: ServerConnection): Pair<JsonNode, Map<String, List<String>>> {
        val node = objectMapper.readTree(conn.received.take())
        assertThat(node.isArray).withFailMessage("선언은 배열이어야 한다: $node").isTrue()
        val map = node.filter { it.has("type") }.associate { it["type"].asText() to it["codes"].map { c -> c.asText() } }
        return node to map
    }

    @Test
    fun `핸드셰이크에 Bearer 토큰을 싣고, 구독 전체를 배열 하나로 선언한다`() {
        val s = newStream()
        s.subscribeTrades(listOf("KRX:005930", "US:AAPL")) { }
        s.subscribeOrderBook(listOf("KRX:005930")) { }
        s.subscribeOrderEvents { }
        s.connect()

        val conn = connections.take()
        assertThat(conn.request.getHeader("Authorization")).isEqualTo("Bearer ACCESS-TOKEN")

        val (node, declared) = nextDeclaration(conn)
        assertThat(node[0].path("id").asText()).startsWith("req-")
        assertThat(declared).containsEntry("trade:kr", listOf("005930"))
        assertThat(declared).containsEntry("trade:us", listOf("AAPL"))
        assertThat(declared).containsEntry("orderbook:kr", listOf("005930"))
        assertThat(declared).containsEntry("personal:order", listOf("3"))
        assertThat(conn.received.poll(300, TimeUnit.MILLISECONDS)).withFailMessage("선언은 한 번만 보내야 한다").isNull()
    }

    @Test
    fun `연결 뒤 추가 구독은 짧게 모아 전체 집합을 다시 선언한다`() {
        val s = newStream()
        s.subscribeTrades(listOf("005930")) { }
        s.connect()
        val conn = connections.take()
        nextDeclaration(conn)

        s.subscribeTrades(listOf("000660")) { }
        s.subscribeOrderBook(listOf("000660")) { }
        val (_, declared) = nextDeclaration(conn)
        assertThat(declared["trade:kr"]).containsExactly("000660", "005930")
        assertThat(declared["orderbook:kr"]).containsExactly("000660")
        assertThat(conn.received.poll(300, TimeUnit.MILLISECONDS)).isNull()
    }

    @Test
    fun `거부된 target 은 다음 선언에서 빠진다`() {
        val s = newStream()
        s.subscribeTrades(listOf("005930", "999999")) { }
        s.connect()
        val conn = connections.take()
        val (_, first) = nextDeclaration(conn)
        assertThat(first["trade:kr"]).containsExactly("005930", "999999")

        conn.socket.send("""{"type":"subscriptions","id":"req-1","subscribed":["trade:kr:005930"],"rejected":[{"target":"trade:kr:999999","code":"stock-not-found","message":"해당 종목을 찾을 수 없습니다."}]}""")
        Thread.sleep(100)
        s.subscribeOrderBook(listOf("005930")) { }
        val (_, second) = nextDeclaration(conn)
        assertThat(second["trade:kr"]).containsExactly("005930")
    }

    @Test
    fun `체결 프레임을 요청 표기 심볼의 틱으로 전달한다 (KRX·US)`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val s = newStream()
        s.subscribeTrades(listOf("KRX:005930", "AAPL".let { "US:$it" })) { ticks.put(it) }
        s.connect()
        val conn = connections.take()
        nextDeclaration(conn)

        conn.socket.send(TRADE_US)
        val us = ticks.take()
        assertThat(us.symbol).isEqualTo("US:AAPL")
        assertThat(us.price).isEqualByComparingTo("243.26")
        assertThat(us.quantity).isEqualByComparingTo("8")
        assertThat(us.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo("23:30")
        assertThat(us.cumulativeVolume).isNull()
        assertThat(us.change).isNull()

        conn.socket.send("""{"type":"message","topic":"trade:kr:005930","data":{"price":"71500","volume":"3","timestamp":"2026-09-14T10:30:00.000+09:00","currency":"KRW"}}""")
        val kr = ticks.take()
        assertThat(kr.symbol).isEqualTo("KRX:005930")
        assertThat(kr.price).isEqualByComparingTo("71500")
    }

    @Test
    fun `접두 없이 구독하면 접두 없이 돌려준다`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val s = newStream()
        s.subscribeTrades(listOf("005930")) { ticks.put(it) }
        s.connect()
        val conn = connections.take()
        nextDeclaration(conn)
        conn.socket.send("""{"type":"message","topic":"trade:kr:005930","data":{"price":"71500","volume":"3","timestamp":"2026-09-14T10:30:00.000+09:00","currency":"KRW"}}""")
        assertThat(ticks.take().symbol).isEqualTo("005930")
    }

    @Test
    fun `호가 프레임 → 호가창`() {
        val books = LinkedBlockingQueue<OrderBookTick>()
        val s = newStream()
        s.subscribeOrderBook(listOf("KRX:005930")) { books.put(it) }
        s.connect()
        val conn = connections.take()
        nextDeclaration(conn)
        conn.socket.send(ORDERBOOK_KR)
        val book = books.take()
        assertThat(book.symbol).isEqualTo("KRX:005930")
        assertThat(book.bestAsk?.price).isEqualByComparingTo("71500")
        assertThat(book.bestAsk?.quantity).isEqualByComparingTo("5")
        assertThat(book.bestBid?.price).isEqualByComparingTo("71400")
        assertThat(book.bestBid?.quantity).isEqualByComparingTo("10")
        assertThat(book.totalAskQuantity).isNull()
    }

    @Test
    fun `주문 이벤트 - FILL 은 체결량·평균가·잔량, 부분 체결은 누적 차이로 계산한다`() {
        val events = LinkedBlockingQueue<OrderEvent>()
        val s = newStream()
        s.subscribeOrderEvents { events.put(it) }
        s.connect()
        val conn = connections.take()
        nextDeclaration(conn)

        conn.socket.send(ORDER_FILL)
        val fill = events.take()
        assertThat(fill.type).isEqualTo(OrderEventType.FILLED)
        assertThat(fill.orderId).isEqualTo("bAGzNvMOOTa5Uy0xVzYNbxDJ3Qpobwau4jDF3hyZZGWbpHm7wha8CFZc7aXVOWAl")
        assertThat(fill.symbol).isEqualTo("US:AAPL")
        assertThat(fill.side).isEqualTo(OrderSide.BUY)
        assertThat(fill.quantity).isEqualByComparingTo("10")
        assertThat(fill.price).isEqualByComparingTo("100")
        assertThat(fill.remainingQuantity).isEqualByComparingTo("0")

        conn.socket.send(orderEvent("ord-2", "PENDING", "PENDING", filled = "0", avg = null))
        assertThat(events.take().type).isEqualTo(OrderEventType.ACCEPTED)
        conn.socket.send(orderEvent("ord-2", "PARTIAL_FILL", "PARTIAL_FILLED", filled = "4", avg = "100"))
        val partial = events.take()
        assertThat(partial.quantity).isEqualByComparingTo("4")
        assertThat(partial.remainingQuantity).isEqualByComparingTo("6")
        conn.socket.send(orderEvent("ord-2", "FILL", "FILLED", filled = "10", avg = "100.5"))
        val rest = events.take()
        assertThat(rest.quantity).isEqualByComparingTo("6")
        assertThat(rest.price).isEqualByComparingTo("100.5")
        assertThat(rest.remainingQuantity).isEqualByComparingTo("0")

        conn.socket.send(orderEvent("ord-3", "CANCELING", "PENDING_CANCEL", filled = "0", avg = null)) // 중간 상태 — 무시
        conn.socket.send(orderEvent("ord-3", "CANCELED", "CANCELED", filled = "0", avg = null))
        val canceled = events.take()
        assertThat(canceled.orderId).isEqualTo("ord-3")
        assertThat(canceled.type).isEqualTo(OrderEventType.CANCELED)
        conn.socket.send(orderEvent("ord-4", "CANCEL_REJECTED", "CANCEL_REJECTED", filled = "0", avg = null))
        val rejected = events.take()
        assertThat(rejected.type).isEqualTo(OrderEventType.REJECTED)
        assertThat(rejected.reason).isEqualTo("CANCEL_REJECTED")
    }

    @Test
    fun `하트비트는 텍스트 PING 을 보내고 pong·error 프레임은 삼킨다`() {
        val s = newStream(heartbeatMillis = 200)
        s.subscribeTrades(listOf("005930")) { }
        s.connect()
        val conn = connections.take()
        nextDeclaration(conn)
        assertThat(conn.received.take()).isEqualTo("PING")
        conn.socket.send("""{"type":"pong"}""")
        conn.socket.send("""{"type":"error","error":{"code":"rate-limit-exceeded","message":"too fast"},"id":"req-9"}""")
        assertThat(conn.received.take()).isEqualTo("PING")
        assertThat(s.isConnected).isTrue()
    }

    @Test
    fun `서버가 끊으면 재접속해 전체 집합을 다시 선언한다`() {
        val s = newStream()
        s.subscribeTrades(listOf("005930")) { }
        s.subscribeOrderEvents { }
        s.connect()
        val first = connections.take()
        nextDeclaration(first)
        first.socket.send("""{"type":"error","error":{"code":"server-shutdown","message":"서버가 재시작됩니다. 재연결해주세요."}}""")
        first.socket.close(1000, "shutdown")

        val second = connections.take(10)
        assertThat(second.request.getHeader("Authorization")).isEqualTo("Bearer ACCESS-TOKEN")
        val (_, declared) = nextDeclaration(second)
        assertThat(declared["trade:kr"]).containsExactly("005930")
        assertThat(declared["personal:order"]).containsExactly("3")
    }

    @Test
    fun `topic 키 - 시장 접두 규칙`() {
        assertThat(TossMarketStream.topicKey("005930")).isEqualTo("kr:005930")
        assertThat(TossMarketStream.topicKey("KRX:005930")).isEqualTo("kr:005930")
        assertThat(TossMarketStream.topicKey("US:aapl")).isEqualTo("us:AAPL")
        org.junit.jupiter.api.assertThrows<IllegalArgumentException> { TossMarketStream.topicKey("JP:7203") }
    }

    @Test
    fun `픽스처 stream 섹션 - 문서 기반 프레임을 같은 모델로 파싱한다`() {
        val fixture = objectMapper.readTree(File("../../conformance/fixtures/toss.json")).path("stream")
        assertThat(fixture.isMissingNode).isFalse()
        assertThat(fixture["measured"].asBoolean()).isFalse()

        val ticks = fixture["frames"].mapNotNull { f -> objectMapper.readTree(f.asText()).let { TossMarketStream.parseTrade(it["topic"].asText(), it["data"]) } }
        assertThat(ticks).hasSize(fixture["expected"].size())
        ticks.zip(fixture["expected"].toList()).forEach { (tick, e) ->
            assertThat(tick.symbol).isEqualTo(e["symbol"].asText())
            assertThat(tick.price).isEqualByComparingTo(e["price"].asText())
            assertThat(tick.quantity).isEqualByComparingTo(e["quantity"].asText())
            assertThat(tick.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
            assertThat(tick.cumulativeVolume).isNull()
            assertThat(tick.change).isNull()
        }

        val book = fixture["orderBook"]
        val books = book["frames"].mapNotNull { f -> objectMapper.readTree(f.asText()).let { TossMarketStream.parseOrderBook(it["topic"].asText(), it["data"]) } }
        assertThat(books).hasSize(book["expected"].size())
        books.zip(book["expected"].toList()).forEach { (b, e) ->
            assertThat(b.symbol).isEqualTo(e["symbol"].asText())
            assertThat(b.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
            fun levels(n: JsonNode) = n.map { it["price"].asText() to it["quantity"].asText() }
            assertThat(b.asks.map { it.price.toPlainString() to it.quantity.toPlainString() }).isEqualTo(levels(e["asks"]))
            assertThat(b.bids.map { it.price.toPlainString() to it.quantity.toPlainString() }).isEqualTo(levels(e["bids"]))
            assertThat(b.totalAskQuantity).isNull()
        }

        val ev = fixture["orderEvents"]
        val events = ev["frames"].mapNotNull { f -> TossMarketStream.parseOrderEvent(objectMapper.readTree(f.asText())["data"], null) }
        assertThat(events).hasSize(ev["expected"].size())
        events.zip(ev["expected"].toList()).forEach { (x, e) ->
            assertThat(x.orderId).isEqualTo(e["orderId"].asText())
            assertThat(x.type.name).isEqualTo(e["type"].asText())
            assertThat(x.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
            assertThat(x.symbol).isEqualTo(e["symbol"].asText())
            assertThat(x.side?.name).isEqualTo(e["side"].asText())
            assertThat(x.quantity).isEqualByComparingTo(e["quantity"].asText())
            assertThat(x.price).isEqualByComparingTo(e["price"].asText())
            assertThat(x.remainingQuantity).isEqualByComparingTo(e["remainingQuantity"].asText())
        }
    }

    companion object {
        /** AsyncAPI 공식 예시 그대로 */
        const val TRADE_US = """{"type":"message","topic":"trade:us:AAPL","data":{"price":"243.26","volume":"8","timestamp":"2026-06-18T23:30:00.000+09:00","currency":"USD"}}"""
        const val ORDERBOOK_KR = """{"type":"message","topic":"orderbook:kr:005930","data":{"timestamp":"2026-06-18T23:30:00.000+09:00","currency":"KRW","asks":[{"price":"71500","volume":"5"}],"bids":[{"price":"71400","volume":"10"}]}}"""
        const val ORDER_FILL = """{"type":"message","topic":"personal:order:3","data":{"event":"FILL","accountSeq":"3","order":{"orderId":"bAGzNvMOOTa5Uy0xVzYNbxDJ3Qpobwau4jDF3hyZZGWbpHm7wha8CFZc7aXVOWAl","symbol":"AAPL","side":"BUY","orderType":"LIMIT","timeInForce":"DAY","status":"FILLED","price":"100.5","quantity":"10","orderAmount":null,"currency":"USD","orderedAt":"2026-06-23T09:30:00.000+09:00","canceledAt":null,"execution":{"filledQuantity":"10","averageFilledPrice":"100","filledAmount":"1000","commission":"1.23","tax":"0","settlementDate":"2026-06-25"}}}}"""

        fun orderEvent(orderId: String, event: String, status: String, filled: String, avg: String?): String =
            """{"type":"message","topic":"personal:order:3","data":{"event":"$event","accountSeq":"3","order":{"orderId":"$orderId","symbol":"005930","side":"BUY","orderType":"LIMIT","timeInForce":"DAY","status":"$status","price":"100","quantity":"10","orderAmount":null,"currency":"KRW","orderedAt":"2026-09-14T10:30:00.000+09:00","canceledAt":null,"execution":{"filledQuantity":"$filled","averageFilledPrice":${avg?.let { "\"$it\"" } ?: "null"},"filledAmount":null,"commission":null,"tax":null,"settlementDate":null}}}}"""
    }
}
