package com.tripleauth.hermetix.client.ls

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.MarketStream
import com.tripleauth.hermetix.broker.MarketSymbol
import com.tripleauth.hermetix.broker.OrderBookLevel
import com.tripleauth.hermetix.broker.OrderBookListener
import com.tripleauth.hermetix.broker.OrderBookTick
import com.tripleauth.hermetix.broker.OrderEvent
import com.tripleauth.hermetix.broker.OrderEventListener
import com.tripleauth.hermetix.broker.OrderEventType
import com.tripleauth.hermetix.broker.ReconnectingWebSocket
import com.tripleauth.hermetix.broker.TradeListener
import com.tripleauth.hermetix.broker.TradeTick
import com.tripleauth.hermetix.client.dto.OrderSide
import io.github.oshai.kotlinlogging.KotlinLogging
import java.math.BigDecimal
import java.math.RoundingMode
import java.net.URI
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * LS증권 OPEN API 실시간 스트림. **문서 기반 구현, 모의 실측 전** — 포털 실시간 TR 문서와 커뮤니티 클라이언트(ebest·LsApiHelper·krsec)에서 역추적.
 *
 * 프로토콜:
 * - 접속: 모의 `wss://openapi.ls-sec.co.kr:29443/websocket`, 실전 `:9443/websocket`. 핸드셰이크 헤더·로그인 프레임 없음
 * - 인증: **매 메시지** `header.token` 에 REST 접근토큰(Bearer 접두 없음). 토큰은 익일 07:00 만료 → 재접속 시 [LsApiClient] 캐시 토큰을 다시 싣는다
 * - 시세 등록/해제: `{"header":{"token","tr_type":"3"|"4"},"body":{"tr_cd":"S3_","tr_key":"005930"}}`
 * - 계좌 등록/해제: `tr_type` `"1"`/`"2"`, `tr_cd` `SC0`(접수)·`SC1`(체결)·`SC2`(정정)·`SC3`(취소)·`SC4`(거부), `tr_key` 빈 문자열. TR 마다 따로 보낸다
 * - 응답: 등록 ACK 는 `header` 에 `rsp_cd`/`rsp_msg` 가 있고 body 가 없다. 데이터는 `{"header":{"tr_cd","tr_key"},"body":{…}}`, 값은 전부 문자열
 * - 하트비트 없음(문서 미기재) — 유휴 감시를 끈다. 서버 PING 프레임에는 JDK 가 자동 PONG
 *
 * **KOSPI/KOSDAQ 선택**: 서버는 종목으로 시장을 고르지 않는다 — KOSDAQ 코드를 `S3_` 에 넣으면 아무것도 오지 않는다. 종목마스터(t8436) 조회 대신
 * 종목마다 KOSPI TR(`S3_`/`H1_`)과 KOSDAQ TR(`K3_`/`HA_`)을 **둘 다 등록**한다. 맞지 않는 쪽은 조용히 비고, 등록 수만 2배가 된다 (한도 미문서).
 *
 * 필드(전부 문자열):
 * - `S3_`/`K3_` 체결: `chetime` HHMMSS KST, `price`, `cvolume` 체결량, `volume` 누적, `change` 부호 없는 대비 + `sign`(4·5 하락), `drate` 부호 있는 %, `offerho`/`bidho`, `shcode`
 * - `H1_`/`HA_` 호가: `hotime`, `offerho1~10`/`bidho1~10`, `offerrem1~10`/`bidrem1~10`, `totofferrem`/`totbidrem`
 * - `SC0` 접수: `ordno`, `orgordno`(신규 `0`), `bnstp`(1 매도/2 매수), `ordqty`, `ordprice`, `ordtm` HHMMSSmmm, `shtcode` A접두
 * - `SC1~SC4`: `ordno`, `orgordno`, `bnstp`, `execqty`/`execprc`(체결), `unercqty` 잔량, `mdfycnfqty`/`mdfycnfprc`(정정), `canccnfqty`(취소), `rjtqty`(거부),
 *   `exectime` HHMMSSmmm, `shtnIsuno` A접두, `ordxctptncode` 11 체결/12 정정/13 취소(/14 거부 추정), `msgcode`
 *
 * 실측 시 확인할 것: ACK JSON 키·`rsp_cd` 값, `SC4` 본문·거부 사유, 세션당 등록 한도, 앱키당 세션 수, 07:00 토큰 만료 시 소켓 동작.
 * 픽스처(문서 재구성값): `conformance/fixtures/ls.json#stream`.
 */
class LsMarketStream(
    private val properties: LsApiProperties,
    private val objectMapper: ObjectMapper,
    private val token: () -> String,
) : ReconnectingWebSocket("ls", idleTimeoutMillis = 0), MarketStream {

    private val logger = KotlinLogging.logger { }

    private val tradeListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<TradeListener>>()
    private val bookListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<OrderBookListener>>()
    private val orderListeners = CopyOnWriteArrayList<OrderEventListener>()

    /** 종목코드 → 구독 요청 표기 */
    private val requestedSymbols = ConcurrentHashMap<String, String>()

    override val isConnected: Boolean get() = isSocketOpen

    override fun uri(): URI = URI.create(properties.resolvedWsUrl())

    override fun onOpen() {
        val t = token()
        tradeListeners.keys.forEach { code -> TRADE_TRS.forEach { send(message(t, TR_TYPE_SUBSCRIBE, it, code)) } }
        bookListeners.keys.forEach { code -> BOOK_TRS.forEach { send(message(t, TR_TYPE_SUBSCRIBE, it, code)) } }
        if (orderListeners.isNotEmpty()) ORDER_TRS.forEach { send(message(t, TR_TYPE_ACCOUNT_REGISTER, it, "")) }
    }

    override fun subscribeTrades(symbols: List<String>, listener: TradeListener) {
        val newCodes = register(symbols, listener, tradeListeners)
        if (newCodes.isNotEmpty() && isSocketOpen) {
            val t = token()
            newCodes.forEach { code -> TRADE_TRS.forEach { send(message(t, TR_TYPE_SUBSCRIBE, it, code)) } }
        }
    }

    override fun subscribeOrderBook(symbols: List<String>, listener: OrderBookListener) {
        val newCodes = register(symbols, listener, bookListeners)
        if (newCodes.isNotEmpty() && isSocketOpen) {
            val t = token()
            newCodes.forEach { code -> BOOK_TRS.forEach { send(message(t, TR_TYPE_SUBSCRIBE, it, code)) } }
        }
    }

    override fun subscribeOrderEvents(listener: OrderEventListener) {
        val first = orderListeners.isEmpty()
        orderListeners += listener
        if (first && isSocketOpen) {
            val t = token()
            ORDER_TRS.forEach { send(message(t, TR_TYPE_ACCOUNT_REGISTER, it, "")) }
        }
    }

    /** 체결 구독 해제 (`tr_type` 4). 리스너도 지운다 */
    fun unsubscribeTrades(symbols: List<String>) {
        val codes = symbols.map { MarketSymbol.code(it) }.filter { tradeListeners.remove(it) != null }
        if (codes.isNotEmpty() && isSocketOpen) {
            val t = token()
            codes.forEach { code -> TRADE_TRS.forEach { send(message(t, TR_TYPE_UNSUBSCRIBE, it, code)) } }
        }
    }

    /** 호가 구독 해제 (`tr_type` 4). 리스너도 지운다 */
    fun unsubscribeOrderBook(symbols: List<String>) {
        val codes = symbols.map { MarketSymbol.code(it) }.filter { bookListeners.remove(it) != null }
        if (codes.isNotEmpty() && isSocketOpen) {
            val t = token()
            codes.forEach { code -> BOOK_TRS.forEach { send(message(t, TR_TYPE_UNSUBSCRIBE, it, code)) } }
        }
    }

    /** 계좌 통보 해제 (`tr_type` 2). 리스너도 지운다 */
    fun unsubscribeOrderEvents() {
        if (orderListeners.isEmpty()) return
        orderListeners.clear()
        if (isSocketOpen) {
            val t = token()
            ORDER_TRS.forEach { send(message(t, TR_TYPE_ACCOUNT_UNREGISTER, it, "")) }
        }
    }

    private fun <L> register(symbols: List<String>, listener: L, target: ConcurrentHashMap<String, CopyOnWriteArrayList<L>>): List<String> {
        val newCodes = mutableListOf<String>()
        symbols.forEach { symbol ->
            val code = MarketSymbol.code(symbol)
            requestedSymbols.putIfAbsent(code, symbol)
            target.computeIfAbsent(code) { newCodes += code; CopyOnWriteArrayList() } += listener
        }
        return newCodes
    }

    override fun onMessage(text: String) {
        val node = runCatching { objectMapper.readTree(text) }.getOrNull() ?: return
        val header = node.path("header")
        val trCd = header.path("tr_cd").asText("")
        val body = node.path("body")
        if (header.has("rsp_msg") || header.has("rsp_cd") || body.isMissingNode || body.isNull) {
            val rspCd = header.path("rsp_cd").asText("")
            val msg = header.path("rsp_msg").asText("")
            if (rspCd.isBlank() || rspCd == RSP_OK) logger.info { "ls stream: ack $trCd ${header.path("tr_key").asText("")} tr_type=${header.path("tr_type").asText("")} ($msg)" }
            else logger.warn { "ls stream: $trCd ${header.path("tr_key").asText("")} rsp_cd=$rspCd $msg" }
            return
        }
        when (trCd) {
            in TRADE_TRS -> parseTrade(node)?.let { tick ->
                val symbol = requestedSymbols[tick.symbol] ?: tick.symbol
                val out = if (symbol == tick.symbol) tick else tick.copy(symbol = symbol)
                tradeListeners[tick.symbol]?.forEach { l -> runCatching { l.onTrade(out) }.onFailure { logger.error(it) { "ls stream: 리스너 오류 / $symbol" } } }
            }
            in BOOK_TRS -> parseOrderBook(node)?.let { tick ->
                val symbol = requestedSymbols[tick.symbol] ?: tick.symbol
                val out = if (symbol == tick.symbol) tick else tick.copy(symbol = symbol)
                bookListeners[tick.symbol]?.forEach { l -> runCatching { l.onOrderBook(out) }.onFailure { logger.error(it) { "ls stream: 호가 리스너 오류 / $symbol" } } }
            }
            in ORDER_TRS -> parseOrderEvents(node).forEach { event ->
                orderListeners.forEach { l -> runCatching { l.onOrderEvent(event) }.onFailure { logger.error(it) { "ls stream: 주문 통보 리스너 오류 / ${event.orderId}" } } }
            }
            else -> logger.debug { "ls stream: unknown tr $trCd" }
        }
    }

    private fun message(token: String, trType: String, trCd: String, trKey: String): String =
        objectMapper.writeValueAsString(
            mapOf(
                "header" to mapOf("token" to token, "tr_type" to trType),
                "body" to mapOf("tr_cd" to trCd, "tr_key" to trKey),
            ),
        )

    companion object {
        const val TR_TRADE_KOSPI = "S3_"
        const val TR_TRADE_KOSDAQ = "K3_"
        const val TR_BOOK_KOSPI = "H1_"
        const val TR_BOOK_KOSDAQ = "HA_"
        const val TR_ORDER_ACCEPTED = "SC0"
        const val TR_ORDER_FILLED = "SC1"
        const val TR_ORDER_MODIFIED = "SC2"
        const val TR_ORDER_CANCELED = "SC3"
        const val TR_ORDER_REJECTED = "SC4"
        val TRADE_TRS = listOf(TR_TRADE_KOSPI, TR_TRADE_KOSDAQ)
        val BOOK_TRS = listOf(TR_BOOK_KOSPI, TR_BOOK_KOSDAQ)
        val ORDER_TRS = listOf(TR_ORDER_ACCEPTED, TR_ORDER_FILLED, TR_ORDER_MODIFIED, TR_ORDER_CANCELED, TR_ORDER_REJECTED)

        const val TR_TYPE_ACCOUNT_REGISTER = "1"
        const val TR_TYPE_ACCOUNT_UNREGISTER = "2"
        const val TR_TYPE_SUBSCRIBE = "3"
        const val TR_TYPE_UNSUBSCRIBE = "4"
        const val RSP_OK = "00000"

        /** xingAPI 관례 sign: 1 상한 2 상승 3 보합 4 하한 5 하락 */
        private val FALLING_SIGNS = setOf("4", "5")
        private val HHMMSS: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmmss")

        private fun JsonNode.text(field: String): String = path(field).asText("").trim()

        private fun JsonNode.decimalOrNull(field: String): BigDecimal? =
            text(field).replace(",", "").takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

        /** `HHMMSS` 또는 `HHMMSSmmm` → 오늘 KST 시각 */
        private fun kstInstant(raw: String, today: LocalDate): Instant {
            val hhmmss = raw.trim().padStart(6, '0').take(6)
            return today.atTime(LocalTime.parse(hhmmss, HHMMSS)).atZone(KrxCalendar.KST).toInstant()
        }

        /** ACK/오류 프레임(header 에 rsp_*·body 없음)이면 true — 데이터 파서는 건너뛴다 */
        private fun isControl(node: JsonNode): Boolean {
            val header = node.path("header")
            val body = node.path("body")
            return header.has("rsp_msg") || header.has("rsp_cd") || body.isMissingNode || body.isNull || body.size() == 0
        }

        /** 시세 프레임의 종목코드 — `header.tr_key` 6자리, 없으면 `body.shcode` */
        private fun marketCode(node: JsonNode): String {
            val key = node.path("header").text("tr_key")
            return if (key.length == 6) key else node.path("body").text("shcode")
        }

        /** 체결 프레임(S3_/K3_) → 체결. 심볼은 단축코드 그대로 (요청 표기 복원은 스트림이 한다). TR 불일치·필수 필드 부족이면 null */
        fun parseTrade(node: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): TradeTick? {
            if (isControl(node) || node.path("header").text("tr_cd") !in TRADE_TRS) return null
            val b = node.path("body")
            return runCatching {
                val sign = b.text("sign")
                val change = b.decimalOrNull("change")?.let { if (sign in FALLING_SIGNS && it.signum() > 0) it.negate() else it }
                TradeTick(
                    symbol = marketCode(node),
                    price = b.decimalOrNull("price") ?: error("no price"),
                    quantity = b.decimalOrNull("cvolume") ?: BigDecimal.ZERO,
                    timestamp = kstInstant(b.text("chetime"), today),
                    askPrice = b.decimalOrNull("offerho"),
                    bidPrice = b.decimalOrNull("bidho"),
                    cumulativeVolume = b.decimalOrNull("volume")?.toLong(),
                    change = change,
                    changeRate = b.decimalOrNull("drate")?.divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN),
                )
            }.getOrNull()
        }

        /** 호가 프레임(H1_/HA_) → 호가창. 0 호가는 빈 단계로 보고 뺀다 */
        fun parseOrderBook(node: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): OrderBookTick? {
            if (isControl(node) || node.path("header").text("tr_cd") !in BOOK_TRS) return null
            val b = node.path("body")
            return runCatching {
                fun levels(pricePrefix: String, qtyPrefix: String): List<OrderBookLevel> =
                    (1..10).mapNotNull { i ->
                        val price = b.decimalOrNull("$pricePrefix$i") ?: return@mapNotNull null
                        if (price.signum() == 0) return@mapNotNull null
                        OrderBookLevel(price, b.decimalOrNull("$qtyPrefix$i") ?: BigDecimal.ZERO)
                    }
                OrderBookTick(
                    symbol = marketCode(node),
                    timestamp = kstInstant(b.text("hotime"), today),
                    asks = levels("offerho", "offerrem"),
                    bids = levels("bidho", "bidrem"),
                    totalAskQuantity = b.decimalOrNull("totofferrem"),
                    totalBidQuantity = b.decimalOrNull("totbidrem"),
                )
            }.getOrNull()
        }

        /** 주문 통보 프레임(SC0~SC4) → 이벤트 목록 (프레임당 1건, 파싱 실패면 빈 목록) */
        fun parseOrderEvents(node: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): List<OrderEvent> {
            val trCd = node.path("header").text("tr_cd")
            if (isControl(node) || trCd !in ORDER_TRS) return emptyList()
            val b = node.path("body")
            return runCatching {
                val orderId = b.text("ordno").ifBlank { error("no ordno") }
                val original = b.text("orgordno").takeIf { it.isNotBlank() && it.trimStart('0').isNotEmpty() && it != orderId }
                val side = when (b.text("bnstp")) { "1" -> OrderSide.SELL; "2" -> OrderSide.BUY; else -> null }
                val symbol = LsApiClient.normalizeCode(b.text("shtnIsuno").ifBlank { b.text("shtcode") }).ifBlank { null }
                val type = when (b.text("ordxctptncode")) {
                    "11" -> OrderEventType.FILLED
                    "12" -> OrderEventType.MODIFIED
                    "13" -> OrderEventType.CANCELED
                    "14" -> OrderEventType.REJECTED
                    else -> when (trCd) {
                        TR_ORDER_FILLED -> OrderEventType.FILLED
                        TR_ORDER_MODIFIED -> OrderEventType.MODIFIED
                        TR_ORDER_CANCELED -> OrderEventType.CANCELED
                        TR_ORDER_REJECTED -> OrderEventType.REJECTED
                        else -> OrderEventType.ACCEPTED
                    }
                }
                val (quantity, price) = when (type) {
                    OrderEventType.ACCEPTED -> b.decimalOrNull("ordqty") to b.decimalOrNull("ordprice")
                    OrderEventType.FILLED -> b.decimalOrNull("execqty") to b.decimalOrNull("execprc")
                    OrderEventType.MODIFIED -> b.decimalOrNull("mdfycnfqty") to b.decimalOrNull("mdfycnfprc")
                    OrderEventType.CANCELED -> b.decimalOrNull("canccnfqty") to b.decimalOrNull("ordprc")
                    OrderEventType.REJECTED -> b.decimalOrNull("rjtqty") to b.decimalOrNull("ordprc")
                }
                val time = if (trCd == TR_ORDER_ACCEPTED) b.text("ordtm") else b.text("exectime")
                listOf(
                    OrderEvent(
                        orderId = orderId,
                        type = type,
                        timestamp = kstInstant(time, today),
                        symbol = symbol,
                        side = side,
                        quantity = quantity,
                        price = price,
                        remainingQuantity = if (trCd == TR_ORDER_ACCEPTED) null else b.decimalOrNull("unercqty"),
                        originalOrderId = original,
                        reason = if (type == OrderEventType.REJECTED) b.text("msgcode").ifBlank { null } else null,
                    ),
                )
            }.getOrDefault(emptyList())
        }
    }
}
