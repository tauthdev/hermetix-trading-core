package com.tripleauth.hermetix.client.nh

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

/** NH PLUG 웹소켓 — 문서 기반 프레임(포털 resExample)으로 구독·ACK·체결·호가·통보·재접속 흐름과 파서를 검증한다 (실측 전) */
class NhMarketStreamTest {

    private val objectMapper = ObjectMapper().registerModule(kotlinModule())
    private lateinit var server: MockWebServer
    private var stream: NhMarketStream? = null
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
                    // OkHttp 서버 소켓은 close 프레임을 받아도 앱이 close() 로 응답해야 핸드셰이크가 끝난다
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

    private fun newStream(marketCd: String = "UNT", accountNo: String = ""): NhMarketStream =
        NhMarketStream(
            NhApiProperties(wsUrl = "ws://${server.hostName}:${server.port}/websocket", marketCd = marketCd, accountNo = accountNo),
            objectMapper,
            token = { "ACCESS-TOKEN" },
        ).also { stream = it }

    private fun <T> LinkedBlockingQueue<T>.take(seconds: Long = 5): T =
        poll(seconds, TimeUnit.SECONDS) ?: error("${seconds}s 안에 도착하지 않음")

    private fun json(text: String): JsonNode = objectMapper.readTree(text)

    @Test
    fun `통합 설정은 mc 로, KRX 설정은 oc 로 토큰을 실어 구독하고 ACK 를 처리한다`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val s = newStream(marketCd = "UNT")
        s.subscribeTrades(listOf("KRX:005940")) { ticks.put(it) }
        s.connect()
        val conn = connections.take()
        val sub = json(conn.received.take())
        assertThat(sub.path("header").path("token").asText()).isEqualTo("ACCESS-TOKEN")
        assertThat(sub.path("header").path("tr_type").asText()).isEqualTo("1")
        assertThat(sub.path("body").path("tr_cd").asText()).isEqualTo("mc")
        assertThat(sub.path("body").path("tr_key").asText()).isEqualTo("005940")

        conn.socket.send("""{"header":{"tr_type":"1","tr_cd":"mc","rsp_cd":"00000","rsp_msg":"정상처리되었습니다"},"body":{"tr_key":["005940"]}}""")
        conn.socket.send(MC_FRAME)
        val tick = ticks.take()
        assertThat(tick.symbol).isEqualTo("KRX:005940")
        assertThat(tick.price).isEqualByComparingTo("31750")
        assertThat(tick.quantity).isEqualByComparingTo("13")
        assertThat(tick.cumulativeVolume).isEqualTo(837624L)
        assertThat(tick.change).isEqualByComparingTo("2500")
        assertThat(tick.changeRate).isEqualByComparingTo("0.0855")
        assertThat(tick.askPrice).isEqualByComparingTo("31750")
        assertThat(tick.bidPrice).isEqualByComparingTo("31700")
        assertThat(s.isConnected).isTrue()
    }

    @Test
    fun `KRX 설정은 oc·ob 채널을 쓴다`() {
        val s = newStream(marketCd = "KRX")
        s.subscribeTrades(listOf("005930")) { }
        s.subscribeOrderBook(listOf("005930")) { }
        s.connect()
        val conn = connections.take()
        val codes = (1..2).map { json(conn.received.take()).path("body").path("tr_cd").asText() }
        assertThat(codes).containsExactlyInAnyOrder("oc", "ob")
    }

    @Test
    fun `호가 mb 프레임을 요청 표기 심볼의 10단계 호가창으로 전달한다`() {
        val books = LinkedBlockingQueue<OrderBookTick>()
        val s = newStream()
        s.subscribeOrderBook(listOf("KRX:005940")) { books.put(it) }
        s.connect()
        val conn = connections.take()
        assertThat(json(conn.received.take()).path("body").path("tr_cd").asText()).isEqualTo("mb")
        conn.socket.send(MB_FRAME)
        val book = books.take()
        assertThat(book.symbol).isEqualTo("KRX:005940")
        assertThat(book.asks).hasSize(10)
        assertThat(book.bids).hasSize(10)
        assertThat(book.bestAsk?.price).isEqualByComparingTo("31800")
        assertThat(book.bestAsk?.quantity).isEqualByComparingTo("668")
        assertThat(book.bestBid?.price).isEqualByComparingTo("31750")
        assertThat(book.bestBid?.quantity).isEqualByComparingTo("2899")
        assertThat(book.asks[9].price).isEqualByComparingTo("32250")
        assertThat(book.bids[9].quantity).isEqualByComparingTo("1171")
        assertThat(book.totalAskQuantity).isEqualByComparingTo("33938")
        assertThat(book.totalBidQuantity).isEqualByComparingTo("13132")
    }

    @Test
    fun `주문 통보는 d2·d3 를 빈 tr_key 로 등록하고 접수→체결 이벤트로 전달한다`() {
        val events = LinkedBlockingQueue<OrderEvent>()
        val s = newStream()
        s.subscribeOrderEvents { events.put(it) }
        s.connect()
        val conn = connections.take()
        val regs = (1..2).map { json(conn.received.take()) }
        assertThat(regs.map { it.path("body").path("tr_cd").asText() }).containsExactlyInAnyOrder("d2", "d3")
        assertThat(regs.map { it.path("body").path("tr_key").asText() }).containsOnly("")

        conn.socket.send(D3_FRAME)
        conn.socket.send(D2_FRAME)
        val accepted = events.take()
        assertThat(accepted.type).isEqualTo(OrderEventType.ACCEPTED)
        assertThat(accepted.orderIdMatches("30")).isTrue()
        assertThat(accepted.symbol).isEqualTo("005940")
        assertThat(accepted.side).isEqualTo(OrderSide.BUY)
        assertThat(accepted.quantity).isEqualByComparingTo("10")
        assertThat(accepted.price).isEqualByComparingTo("35550")
        val filled = events.take()
        assertThat(filled.type).isEqualTo(OrderEventType.FILLED)
        assertThat(filled.quantity).isEqualByComparingTo("5")
        assertThat(filled.price).isEqualByComparingTo("35550")
        assertThat(filled.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo("11:56:06")
    }

    @Test
    fun `서버가 끊으면 재접속해 구독과 통보 등록을 다시 보낸다`() {
        val s = newStream()
        s.subscribeTrades(listOf("005940")) { }
        s.subscribeOrderEvents { }
        s.connect()
        val first = connections.take()
        repeat(3) { first.received.take() }
        first.socket.close(1000, "bye")

        val second = connections.take(10)
        val codes = (1..3).map { json(second.received.take()).path("body").path("tr_cd").asText() }
        assertThat(codes).containsExactlyInAnyOrder("mc", "d2", "d3")
    }

    @Test
    fun `연결된 뒤 추가 구독은 즉시 전송된다`() {
        val s = newStream()
        s.connect()
        val conn = connections.take()
        s.subscribeTrades(listOf("000660")) { }
        assertThat(json(conn.received.take()).path("body").path("tr_key").asText()).isEqualTo("000660")
    }

    @Test
    fun `파서 - 숫자 타입·HHMMSS 시각·하락 부호·itemgb 필터·계좌 필터`() {
        val numeric = json("""{"header":{"tr_cd":"oc","tr_key":"005930"},"body":{"code":"005930","time":"140031","sign":"5","change":1200,"price":71500,"chrate":1.65,"offer":71600,"bid":71500,"movolume":7,"new_volume":123456}}""")
        val t = NhMarketStream.parseTrade(numeric, today).single()
        assertThat(t.price).isEqualByComparingTo("71500")
        assertThat(t.change).isEqualByComparingTo("-1200") // sign 5 하락 → 음수
        assertThat(t.changeRate).isEqualByComparingTo("-0.0165")
        assertThat(t.quantity).isEqualByComparingTo("7")
        assertThat(t.cumulativeVolume).isEqualTo(123456L)
        assertThat(t.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo("14:00:31")

        val futures = json("""{"header":{"tr_cd":"d2"},"body":{"itemgb":"2","orderno":"1","issuecd":"101S3000","slbygb":"2","concgty":"1","concprc":"100","conctime":"100000","ucgb":"0","rejgb":"0"}}""")
        assertThat(NhMarketStream.parseOrderEvents(futures, today)).isEmpty()

        val otherAccount = json("""{"header":{"tr_cd":"d2"},"body":{"itemgb":"1","accountno":"99999999999","orderno":"1","issuecd":"005930","slbygb":"2","concgty":"1","concprc":"100","conctime":"100000","ucgb":"0","rejgb":"0"}}""")
        assertThat(NhMarketStream.parseOrderEvents(otherAccount, today, accountNo = "11111111111")).isEmpty()
        assertThat(NhMarketStream.parseOrderEvents(otherAccount, today, accountNo = "")).hasSize(1)

        val canceled = json("""{"header":{"tr_cd":"d2"},"body":{"itemgb":"1","orderno":"0000000031","issuecd":"A005930","slbygb":"1","concgty":"0000000000","concprc":"00000000000","conctime":"100000","ucgb":"2","rejgb":"0"}}""")
        val c = NhMarketStream.parseOrderEvents(canceled, today).single()
        assertThat(c.type).isEqualTo(OrderEventType.CANCELED)
        assertThat(c.side).isEqualTo(OrderSide.SELL)
        assertThat(c.symbol).isEqualTo("005930")
        val rejected = json("""{"header":{"tr_cd":"d2"},"body":{"itemgb":"1","orderno":"32","issuecd":"005930","slbygb":"2","concgty":"0","concprc":"0","conctime":"100000","ucgb":"0","rejgb":"1"}}""")
        assertThat(NhMarketStream.parseOrderEvents(rejected, today).single().type).isEqualTo(OrderEventType.REJECTED)
        val modifyAccept = json("""{"header":{"tr_cd":"d3"},"body":{"itemgb":"1","orderno":"0000000033","orgordno":"0000000030","issuecd":"005930","slbygb":"2","ordergty":"5","orderprc":"71000","order_time":"100000"}}""")
        val m = NhMarketStream.parseOrderEvents(modifyAccept, today).single()
        assertThat(m.type).isEqualTo(OrderEventType.ACCEPTED)
        assertThat(m.originalOrderId).isEqualTo("0000000030")
    }

    // ------------------------------------------------------------------ 픽스처 (네 언어 공용)

    private fun fixture(): JsonNode {
        val file = File("../../conformance/fixtures/nh.json")
        check(file.exists()) { "픽스처가 없다: ${file.absolutePath}" }
        return objectMapper.readTree(file).path("stream").also { check(!it.isMissingNode) { "nh 픽스처에 stream 섹션이 없다" } }
    }

    @Test
    fun `픽스처 - 체결 프레임`() {
        val section = fixture()
        assertThat(section["measured"].asBoolean()).isFalse()
        val ticks = section["frames"].flatMap { NhMarketStream.parseTrade(json(it.asText()), today) }
        val expected = section["expected"]
        assertThat(ticks).hasSize(expected.size())
        ticks.zip(expected.toList()).forEach { (tick, e) ->
            assertThat(tick.symbol).isEqualTo(e["symbol"].asText())
            assertThat(tick.price).isEqualByComparingTo(e["price"].asText())
            assertThat(tick.quantity).isEqualByComparingTo(e["quantity"].asText())
            assertThat(tick.askPrice).isEqualByComparingTo(e["askPrice"].asText())
            assertThat(tick.bidPrice).isEqualByComparingTo(e["bidPrice"].asText())
            assertThat(tick.cumulativeVolume).isEqualTo(e["cumulativeVolume"].asLong())
            assertThat(tick.change).isEqualByComparingTo(e["change"].asText())
            assertThat(tick.changeRate).isEqualByComparingTo(e["changeRate"].asText())
            assertThat(tick.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
        }
    }

    @Test
    fun `픽스처 - 호가 프레임`() {
        val section = fixture()["orderBook"]
        assertThat(section["measured"].asBoolean()).isFalse()
        val books = section["frames"].flatMap { NhMarketStream.parseOrderBook(json(it.asText()), today) }
        val expected = section["expected"]
        assertThat(books).hasSize(expected.size())
        books.zip(expected.toList()).forEach { (book, e) ->
            assertThat(book.symbol).isEqualTo(e["symbol"].asText())
            assertThat(book.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
            fun levels(node: JsonNode) = node.map { it["price"].asText() to it["quantity"].asText() }
            assertThat(book.asks.map { it.price.toPlainString() to it.quantity.toPlainString() }).isEqualTo(levels(e["asks"]))
            assertThat(book.bids.map { it.price.toPlainString() to it.quantity.toPlainString() }).isEqualTo(levels(e["bids"]))
            assertThat(book.totalAskQuantity).isEqualByComparingTo(e["totalAskQuantity"].asText())
            assertThat(book.totalBidQuantity).isEqualByComparingTo(e["totalBidQuantity"].asText())
        }
    }

    @Test
    fun `픽스처 - 주문 통보 프레임`() {
        val section = fixture()["orderEvents"]
        assertThat(section["measured"].asBoolean()).isFalse()
        val events = section["frames"].flatMap { NhMarketStream.parseOrderEvents(json(it.asText()), today) }
        val expected = section["expected"]
        assertThat(events).hasSize(expected.size())
        events.zip(expected.toList()).forEach { (ev, e) ->
            assertThat(ev.orderId).isEqualTo(e["orderId"].asText())
            assertThat(ev.type.name).isEqualTo(e["type"].asText())
            assertThat(ev.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
            assertThat(ev.symbol).isEqualTo(e["symbol"].asText())
            assertThat(ev.side?.name).isEqualTo(e["side"].asText())
            assertThat(ev.quantity).isEqualByComparingTo(e["quantity"].asText())
            assertThat(ev.price).isEqualByComparingTo(e["price"].asText())
            if (e.has("originalOrderId")) assertThat(ev.originalOrderId).isEqualTo(e["originalOrderId"].asText()) else assertThat(ev.originalOrderId).isNull()
        }
    }

    companion object {
        /** 포털 API 가이드 resExample 원문 (mc/mb/d2/d3) — 문서 기반 */
        const val MC_FRAME = """{"header": {"tr_cd": "mc", "tr_key": "005940"}, "body": {"code": "005940", "time": "14:00:31", "sign": "2", "change": "2500", "price": "31750", "chrate": "8.55", "high": "32200", "low": "29450", "offer": "31750", "bid": "31700", "volume": "837624", "volrate": "81.75", "movolume": "13", "value": "26387", "open": "29600", "avgprice": "31503", "janggubun": "0", "bidrate": "57.65", "volpower": "137.24", "new_volume": "837624", "bidvolall": "482854", "offvolall": "351828", "kospigb": "1", "value_won": "26387679000", "marketgb": "1", "main_close": "0", "market_sign": "0", "market_change": "0", "market_chrate": "0.00"}}"""
        const val MB_FRAME = """{"header": {"tr_cd": "mb", "tr_key": "005940"}, "body": {"code": "005940", "hotime": "13:55:11", "offer": "31800", "bid": "31750", "offerrem": "668", "bidrem": "2899", "P_offer": "31850", "P_bid": "31700", "P_offerrem": "289", "P_bidrem": "1582", "S_offer": "31900", "S_bid": "31650", "S_offerrem": "40", "S_bidrem": "1508", "S4_offer": "31950", "S4_bid": "31600", "S4_offerrem": "317", "S4_bidrem": "2224", "S5_offer": "32000", "S5_bid": "31550", "S5_offerrem": "2923", "S5_bidrem": "659", "T_offerrem": "33938", "T_bidrem": "13132", "S6_offer": "32050", "S6_bid": "31500", "S6_offerrem": "3329", "S6_bidrem": "788", "S7_offer": "32100", "S7_bid": "31450", "S7_offerrem": "5659", "S7_bidrem": "918", "S8_offer": "32150", "S8_bid": "31400", "S8_offerrem": "4708", "S8_bidrem": "776", "S9_offer": "32200", "S9_bid": "31350", "S9_offerrem": "10399", "S9_bidrem": "607", "S10_offer": "32250", "S10_bid": "31300", "S10_offerrem": "5606", "S10_bidrem": "1171", "volume": "0", "krx_mid_prc": "31775", "krx_mid_offerrem": "0", "krx_mid_bidrem": "0", "nxt_mid_prc": "31775", "nxt_mid_offerrem": "0", "nxt_mid_bidrem": "0", "kospigb": "1"}}"""
        const val D3_FRAME = """{"header": {"tr_cd": "d3"}, "body": {"userid": "ID", "itemgb": "1", "accountno": "ACCOUNT NUMBER", "orderno": "0000000030", "orgordno": "", "ordercd": "10", "issuecd": "005940", "issuename": "NH투자증권", "slbygb": "2", "order_type": "01", "ordergty": "0000000010", "orderprc": "00000035550", "procnm": "00", "commcd": "F5", "order_cond": "0", "fundcode": "000", "sin_gb": "10", "order_time": "115606", "loan_date": "", "rmt_mkt_cd": "S", "snd_mkt_cd": "K", "ord_cond_prc": "00000000000", "sor_orrgb": "B", "mkt_order_qty": "0000000010"}}"""
        const val D2_FRAME = """{"header": {"tr_cd": "d2"}, "body": {"userid": "ID", "itemgb": "1", "accountno": "ACCOUNT NUMBER", "orderno": "0000000030", "issuecd": "005940", "slbygb": "2", "concgty": "0000000005", "concprc": "00000035550", "conctime": "115606", "ucgb": "0", "rejgb": "0", "fundcode": "000", "sin_gb": "10", "loan_date": "", "ato_ord_tpe_chg": "0", "issue_nm": "NH투자증권", "rmt_mkt_cd": "S", "snd_mkt_cd": "K", "ord_cond_prc": "", "sor_orrgb": "B", "stop_efforn_gb": ""}}"""
    }
}
