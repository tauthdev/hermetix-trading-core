package com.tripleauth.hermetix.client.ls

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
import java.time.ZoneId
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/** LS증권 웹소켓 — 가짜 서버로 등록·ACK·프레임 파싱·재접속 흐름 + 픽스처(문서 재구성값) 파서 일치 */
class LsMarketStreamTest {

    private val objectMapper = ObjectMapper().registerModule(kotlinModule())
    private lateinit var server: MockWebServer
    private var stream: LsMarketStream? = null

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

    private fun newStream(): LsMarketStream =
        LsMarketStream(
            LsApiProperties(wsUrl = "ws://${server.hostName}:${server.port}/websocket"),
            objectMapper,
            token = { "ACCESS-TOKEN" },
        ).also { stream = it }

    private fun <T> LinkedBlockingQueue<T>.take(seconds: Long = 5): T =
        poll(seconds, TimeUnit.SECONDS) ?: error("${seconds}s 안에 도착하지 않음")

    /** n 개의 메시지를 받아 (tr_type, tr_cd, tr_key) 로 정리 */
    private fun ServerConnection.takeMessages(n: Int): List<Triple<String, String, String>> =
        (1..n).map {
            val m = objectMapper.readTree(received.take())
            assertThat(m.path("header").path("token").asText()).isEqualTo("ACCESS-TOKEN")
            Triple(m.path("header").path("tr_type").asText(), m.path("body").path("tr_cd").asText(), m.path("body").path("tr_key").asText())
        }

    @Test
    fun `종목 하나를 구독하면 KOSPI·KOSDAQ 체결 TR 을 tr_type 3 으로 둘 다 등록하고, 체결 프레임을 요청 표기로 전달한다`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val s = newStream()
        s.subscribeTrades(listOf("KRX:005930")) { ticks.put(it) }
        s.connect()
        val conn = connections.take()
        val msgs = conn.takeMessages(2)
        assertThat(msgs).containsExactlyInAnyOrder(Triple("3", "S3_", "005930"), Triple("3", "K3_", "005930"))

        conn.socket.send(ACK_S3)
        conn.socket.send(S3_FRAME)
        val tick = ticks.take()
        assertThat(tick.symbol).isEqualTo("KRX:005930")
        assertThat(tick.price).isEqualByComparingTo("55550")
        assertThat(tick.quantity).isEqualByComparingTo("1")
        assertThat(tick.cumulativeVolume).isEqualTo(10887L)
        assertThat(tick.change).isEqualByComparingTo("1050")
        assertThat(tick.changeRate).isEqualByComparingTo("0.0193")
        assertThat(tick.askPrice).isEqualByComparingTo("55600")
        assertThat(tick.bidPrice).isEqualByComparingTo("55500")
        assertThat(s.isConnected).isTrue()

        // KOSDAQ TR 로 같은 레이아웃이 와도 전달된다
        conn.socket.send(S3_FRAME.replace("\"tr_cd\":\"S3_\"", "\"tr_cd\":\"K3_\""))
        assertThat(ticks.take().symbol).isEqualTo("KRX:005930")
    }

    @Test
    fun `주문 통보를 구독하면 SC0~SC4 를 tr_type 1·빈 tr_key 로 등록하고, 접수·체결·취소 프레임을 이벤트로 전달한다`() {
        val events = LinkedBlockingQueue<OrderEvent>()
        val s = newStream()
        s.subscribeOrderEvents { events.put(it) }
        s.connect()
        val conn = connections.take()
        val msgs = conn.takeMessages(5)
        assertThat(msgs.map { it.second }).containsExactly("SC0", "SC1", "SC2", "SC3", "SC4")
        assertThat(msgs.map { it.first }.toSet()).containsExactly("1")
        assertThat(msgs.map { it.third }.toSet()).containsExactly("")

        conn.socket.send(SC0_FRAME)
        val accepted = events.take()
        assertThat(accepted.type).isEqualTo(OrderEventType.ACCEPTED)
        assertThat(accepted.orderId).isEqualTo("86382")
        assertThat(accepted.side).isEqualTo(OrderSide.BUY)
        assertThat(accepted.quantity).isEqualByComparingTo("2")
        assertThat(accepted.price).isEqualByComparingTo("60000")
        assertThat(accepted.symbol).isEqualTo("005930")
        assertThat(accepted.originalOrderId).isNull()

        conn.socket.send(SC1_FRAME)
        val filled = events.take()
        assertThat(filled.type).isEqualTo(OrderEventType.FILLED)
        assertThat(filled.orderId).isEqualTo("86382")
        assertThat(filled.quantity).isEqualByComparingTo("1")
        assertThat(filled.price).isEqualByComparingTo("60000")
        assertThat(filled.remainingQuantity).isEqualByComparingTo("1")

        conn.socket.send(SC3_FRAME)
        val canceled = events.take()
        assertThat(canceled.type).isEqualTo(OrderEventType.CANCELED)
        assertThat(canceled.orderId).isEqualTo("88343")
        assertThat(canceled.originalOrderId).isEqualTo("88342")
        assertThat(canceled.symbol).isEqualTo("000020")
    }

    @Test
    fun `호가 구독은 H1_·HA_ 를 등록하고 호가 프레임을 10단계 호가창으로 전달한다`() {
        val books = LinkedBlockingQueue<OrderBookTick>()
        val s = newStream()
        s.subscribeOrderBook(listOf("KRX:005930")) { books.put(it) }
        s.connect()
        val conn = connections.take()
        assertThat(conn.takeMessages(2)).containsExactlyInAnyOrder(Triple("3", "H1_", "005930"), Triple("3", "HA_", "005930"))

        conn.socket.send(H1_FRAME)
        val book = books.take()
        assertThat(book.symbol).isEqualTo("KRX:005930")
        assertThat(book.asks).hasSize(10)
        assertThat(book.bids).hasSize(10)
        assertThat(book.bestAsk?.price).isEqualByComparingTo("72400")
        assertThat(book.bestAsk?.quantity).isEqualByComparingTo("32616")
        assertThat(book.bestBid?.price).isEqualByComparingTo("72300")
        assertThat(book.bestBid?.quantity).isEqualByComparingTo("70581")
        assertThat(book.asks[9].price).isEqualByComparingTo("73300")
        assertThat(book.bids[9].price).isEqualByComparingTo("71400")
        assertThat(book.totalAskQuantity).isEqualByComparingTo("400000")
        assertThat(book.totalBidQuantity).isEqualByComparingTo("500000")
        assertThat(book.timestamp.atZone(ZoneId.of("Asia/Seoul")).toLocalTime().toString()).isEqualTo("08:42:42")
    }

    @Test
    fun `서버가 끊으면 재접속해 모든 등록을 다시 보낸다`() {
        val s = newStream()
        s.subscribeTrades(listOf("005930")) { }
        s.subscribeOrderEvents { }
        s.connect()
        val first = connections.take()
        first.takeMessages(7)
        first.socket.close(1000, "bye")

        val second = connections.take(10)
        val msgs = second.takeMessages(7)
        assertThat(msgs.map { it.second }).containsExactlyInAnyOrder("S3_", "K3_", "SC0", "SC1", "SC2", "SC3", "SC4")
    }

    @Test
    fun `연결된 뒤 추가 구독은 즉시, 해제는 tr_type 4·2 로 전송된다`() {
        val s = newStream()
        s.connect()
        val conn = connections.take()
        s.subscribeTrades(listOf("000660")) { }
        assertThat(conn.takeMessages(2)).containsExactlyInAnyOrder(Triple("3", "S3_", "000660"), Triple("3", "K3_", "000660"))
        s.unsubscribeTrades(listOf("KRX:000660"))
        assertThat(conn.takeMessages(2)).containsExactlyInAnyOrder(Triple("4", "S3_", "000660"), Triple("4", "K3_", "000660"))
        s.unsubscribeTrades(listOf("000660")) // 이미 해제 — 전송 없음
        s.subscribeOrderEvents { }
        conn.takeMessages(5)
        s.unsubscribeOrderEvents()
        assertThat(conn.takeMessages(5).map { it.first }.toSet()).containsExactly("2")
    }

    @Test
    fun `ACK·오류 프레임은 데이터로 처리하지 않는다`() {
        val today = LocalDate.of(2026, 9, 14)
        assertThat(LsMarketStream.parseTrade(objectMapper.readTree(ACK_S3), today)).isNull()
        assertThat(LsMarketStream.parseOrderEvents(objectMapper.readTree("""{"header":{"tr_cd":"SC1","rsp_cd":"IGW00121","rsp_msg":"유효하지 않은 token 입니다."}}"""), today)).isEmpty()
        assertThat(LsMarketStream.parseTrade(objectMapper.readTree(H1_FRAME), today)).isNull() // TR 불일치
    }

    @Test
    fun `주문 통보 파싱 - 정정·거부·ordxctptncode 우선`() {
        val today = LocalDate.of(2026, 9, 14)
        val modified = LsMarketStream.parseOrderEvents(objectMapper.readTree(SC2_FRAME), today).single()
        assertThat(modified.type).isEqualTo(OrderEventType.MODIFIED)
        assertThat(modified.orderId).isEqualTo("86383")
        assertThat(modified.originalOrderId).isEqualTo("86382")
        assertThat(modified.quantity).isEqualByComparingTo("1")
        assertThat(modified.price).isEqualByComparingTo("70000")
        assertThat(modified.timestamp.atZone(ZoneId.of("Asia/Seoul")).toLocalTime().toString()).isEqualTo("10:00:45")

        val rejected = LsMarketStream.parseOrderEvents(objectMapper.readTree(SC4_FRAME), today).single()
        assertThat(rejected.type).isEqualTo(OrderEventType.REJECTED)
        assertThat(rejected.quantity).isEqualByComparingTo("2")
        assertThat(rejected.reason).isEqualTo("0040")

        // tr_cd 는 SC1 이지만 ordxctptncode 13 → 취소
        val byCode = LsMarketStream.parseOrderEvents(objectMapper.readTree(SC3_FRAME.replace("\"tr_cd\":\"SC3\"", "\"tr_cd\":\"SC1\"")), today).single()
        assertThat(byCode.type).isEqualTo(OrderEventType.CANCELED)
    }

    // ------------------------------------------------------------------ 픽스처 (문서 재구성값, measured=false)

    private fun fixture(): JsonNode {
        val file = File("../../conformance/fixtures/ls.json")
        check(file.exists()) { "픽스처가 없다: ${file.absolutePath}" }
        return objectMapper.readTree(file).path("stream").also { check(!it.isMissingNode) { "ls 픽스처에 stream 섹션이 없다" } }
    }

    private fun time(instant: java.time.Instant) = instant.atZone(KrxCalendar.KST).toLocalTime().toString()

    @Test
    fun `픽스처 - 체결 프레임`() {
        val today = LocalDate.of(2026, 9, 14)
        val section = fixture()
        assertThat(section["measured"].asBoolean()).isFalse()
        val ticks = section["frames"].mapNotNull { LsMarketStream.parseTrade(objectMapper.readTree(it.asText()), today) }
        val expected = section["expected"]
        assertThat(ticks).hasSize(expected.size())
        ticks.zip(expected.toList()).forEach { (tick, e) ->
            assertThat(tick.symbol).isEqualTo(e["symbol"].asText())
            assertThat(tick.price).isEqualByComparingTo(e["price"].asText())
            assertThat(tick.quantity).isEqualByComparingTo(e["quantity"].asText())
            assertThat(time(tick.timestamp)).isEqualTo(e["time"].asText())
            assertThat(tick.askPrice).isEqualByComparingTo(e["askPrice"].asText())
            assertThat(tick.bidPrice).isEqualByComparingTo(e["bidPrice"].asText())
            assertThat(tick.cumulativeVolume).isEqualTo(e["cumulativeVolume"].asLong())
            assertThat(tick.change).isEqualByComparingTo(e["change"].asText())
            assertThat(tick.changeRate).isEqualByComparingTo(e["changeRate"].asText())
        }
    }

    @Test
    fun `픽스처 - 호가 프레임`() {
        val today = LocalDate.of(2026, 9, 14)
        val section = fixture()["orderBook"]
        assertThat(section["measured"].asBoolean()).isFalse()
        val books = section["frames"].mapNotNull { LsMarketStream.parseOrderBook(objectMapper.readTree(it.asText()), today) }
        val expected = section["expected"]
        assertThat(books).hasSize(expected.size())
        books.zip(expected.toList()).forEach { (book, e) ->
            assertThat(book.symbol).isEqualTo(e["symbol"].asText())
            assertThat(time(book.timestamp)).isEqualTo(e["time"].asText())
            fun levels(node: JsonNode) = node.map { it["price"].asText() to it["quantity"].asText() }
            assertThat(book.asks.map { it.price.toPlainString() to it.quantity.toPlainString() }).isEqualTo(levels(e["asks"]))
            assertThat(book.bids.map { it.price.toPlainString() to it.quantity.toPlainString() }).isEqualTo(levels(e["bids"]))
            assertThat(book.totalAskQuantity).isEqualByComparingTo(e["totalAskQuantity"].asText())
            assertThat(book.totalBidQuantity).isEqualByComparingTo(e["totalBidQuantity"].asText())
        }
    }

    @Test
    fun `픽스처 - 주문 통보 프레임`() {
        val today = LocalDate.of(2026, 9, 14)
        val section = fixture()["orderEvents"]
        assertThat(section["measured"].asBoolean()).isFalse()
        val events = section["frames"].flatMap { LsMarketStream.parseOrderEvents(objectMapper.readTree(it.asText()), today) }
        val expected = section["expected"]
        assertThat(events).hasSize(expected.size())
        events.zip(expected.toList()).forEach { (ev, e) ->
            assertThat(ev.orderId).isEqualTo(e["orderId"].asText())
            assertThat(ev.type.name).isEqualTo(e["type"].asText())
            assertThat(time(ev.timestamp)).isEqualTo(e["time"].asText())
            assertThat(ev.symbol).isEqualTo(e["symbol"].asText())
            assertThat(ev.side?.name).isEqualTo(e["side"].asText())
            assertThat(ev.quantity).isEqualByComparingTo(e["quantity"].asText())
            assertThat(ev.price).isEqualByComparingTo(e["price"].asText())
            if (e.has("remainingQuantity")) assertThat(ev.remainingQuantity).isEqualByComparingTo(e["remainingQuantity"].asText())
            if (e.has("originalOrderId")) assertThat(ev.originalOrderId).isEqualTo(e["originalOrderId"].asText()) else assertThat(ev.originalOrderId).isNull()
        }
    }

    companion object {
        /** 포털 S3_ 공식 예시 그대로 */
        const val S3_FRAME = """{"header":{"tr_cd":"S3_","tr_key":"005930"},"body":{"mdchecnt":"23","sign":"2","mschecnt":"96","mdvolume":"946","w_avrg":"55448","cpower":"332.56","offerho":"55600","cvolume":"1","high":"55800","bidho":"55500","low":"55500","price":"55550","cgubun":"+","value":"604","change":"1050","shcode":"005930","chetime":"090851","opentime":"090030","lowtime":"090030","volume":"10887","drate":"1.93","hightime":"090504","jnilvolume":"2508350","msvolume":"3146","exchname":"KRX","open":"55600","status":"00"}}"""

        const val ACK_S3 = """{"header":{"tr_cd":"S3_","tr_key":"005930","tr_type":"3","rsp_cd":"00000","rsp_msg":"정상처리되었습니다"}}"""

        /** 포털 H1_ 예시(08:42:42, 1호가 72400/72300, 잔량 32616/70581)를 10단계로 확장 */
        val H1_FRAME: String = run {
            val v = mutableMapOf("hotime" to "084242", "shcode" to "005930", "donsigubun" to "3", "totofferrem" to "400000", "totbidrem" to "500000", "volume" to "136", "alloc_gubun" to "")
            for (i in 1..10) {
                v["offerho$i"] = (72400 + 100 * (i - 1)).toString(); v["offerrem$i"] = (32616 + 1000 * (i - 1)).toString()
                v["bidho$i"] = (72300 - 100 * (i - 1)).toString(); v["bidrem$i"] = (70581 + 1000 * (i - 1)).toString()
            }
            val body = v.entries.joinToString(",") { "\"${it.key}\":\"${it.value}\"" }
            """{"header":{"tr_cd":"H1_","tr_key":"005930"},"body":{$body}}"""
        }

        /** 포털 SC0 공식 예시 — 업무 필드 발췌 */
        const val SC0_FRAME = """{"header":{"tr_cd":"SC0"},"body":{"ordchegb":"01","marketgb":"10","ordgb":"02","orgordno":"0","accno1":"20011132702","accno":"20011132702","expcode":"KR7005930003","shtcode":"A005930","hname":"삼성전자","ordqty":"2","ordprice":"60000","hogagb":"0","etfhogagb":"00","singb":"000","ordno":"86382","ordtm":"095636020","prntordno":"86382","ordamt":"120000","bnstp":"2","deposit":"79759964","ordablemny":"79459964","trcode":"SONAT000","msgcode":"0040"}}"""

        /** 포털 SC1 공식 예시 — 업무 필드 발췌 */
        const val SC1_FRAME = """{"header":{"tr_cd":"SC1"},"body":{"ordxctptncode":"11","ordno":"86382","orgordno":"0","execno":"1","shtnIsuno":"A005930","Isuno":"KR7005930003","Isunm":"삼성전자","bnstp":"2","ordqty":"2","ordprc":"60000","execqty":"1","execprc":"60000","ordavrexecprc":"60000","unercqty":"1","mdfycnfqty":"0","mdfycnfprc":"0","canccnfqty":"0","rjtqty":"0","exectime":"095636107","rcptexectime":"095636098","ordprcptncode":"00","ordptncode":"02","ordmktcode":"10","mgntrncode":"000","ordamt":"120000","mnyexecamt":"60000","deposit":"79759964","ordablemny":"79459964","flctqty":"1","accno":"20011132702","trcode":"SONAS100","msgcode":"9999"}}"""

        /** 포털 SC2 공식 예시 — 정정 확인 (새 주문번호 86383, 원주문 86382) */
        const val SC2_FRAME = """{"header":{"tr_cd":"SC2"},"body":{"ordxctptncode":"12","ordno":"86383","orgordno":"86382","execno":"0","shtnIsuno":"A005930","Isunm":"삼성전자","bnstp":"2","ordqty":"1","ordprc":"70000","execqty":"0","execprc":"0","unercqty":"1","mdfycnfqty":"1","mdfycnfprc":"70000","canccnfqty":"0","rjtqty":"0","orgordmdfyqty":"1","exectime":"100045203","ordtrxptncode":"6","trcode":"SONAS100","msgcode":"9999","outgu":"1"}}"""

        /** 포털 SC3 공식 예시 — 취소 확인 (취소 주문번호 88343, 원주문 88342) */
        const val SC3_FRAME = """{"header":{"tr_cd":"SC3"},"body":{"ordxctptncode":"13","ordno":"88343","orgordno":"88342","execno":"0","shtnIsuno":"A000020","bnstp":"2","ordqty":"1","ordprc":"0","execqty":"0","execprc":"0","unercqty":"0","mdfycnfqty":"0","mdfycnfprc":"0","canccnfqty":"1","rjtqty":"0","orgordunercqty":"5","orgordcancqty":"1","exectime":"150622765","ordtrxptncode":"0","trcode":"SONAS100","msgcode":"9999","outgu":"1"}}"""

        /** SC4 거부 — 포털에 필드표 없음, SC1 레이아웃 가정 + rjtqty·msgcode */
        const val SC4_FRAME = """{"header":{"tr_cd":"SC4"},"body":{"ordxctptncode":"14","ordno":"86390","orgordno":"0","shtnIsuno":"A005930","bnstp":"2","ordqty":"2","ordprc":"1","execqty":"0","execprc":"0","unercqty":"0","mdfycnfqty":"0","canccnfqty":"0","rjtqty":"2","exectime":"101010000","trcode":"SONAS100","msgcode":"0040"}}"""
    }
}
