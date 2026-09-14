package com.tripleauth.hermetix.client.db

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
 * DB증권 실시간 스트림 — 체결 `S00`, 호가 `S01`, 주문 접수 `IS0`, 주문 체결 `IS1`. **문서 기반 구현, 실측 전.**
 *
 * 프로토콜 (포털 [실시간] 문서 + 공식 SDK `DBsecurities/dbsec-open-api`):
 * - 접속: 운영 `wss://openapi.dbsec.co.kr:7070/websocket`, 모의 `:17070/websocket`. 핸드셰이크 헤더 없음
 * - 인증: 로그인 프레임 없이 **매 메시지 `header.token`** 에 REST 접근토큰(Bearer 접두 없음)
 * - 시세 등록 `{"header":{"token","tr_type":"1"},"body":{"tr_cd":"S00","tr_key":"J 005930"}}`, 해제는 `tr_type:"2"`.
 *   `tr_key` = 시장구분 2자리(`J` + 공백) + 종목코드. NXT 는 `NJN-005930`, 통합은 `UJU-005930`
 * - 계좌 등록 `{"header":{"token","tr_type":"3"},"body":{"tr_cd":"IS0"}}` — `tr_key` 없음, 해제 메시지 없음(세션 종료가 해제)
 * - **접속 후 10초 안에 첫 메시지를 보내야 한다** → 구독이 있으면 onOpen 에서 즉시 전송. 서버 주기 프레임이 없어 유휴 감시는 끈다
 * - 응답: 구독 ack `{"header":{"tr_cd":"S00","tr_key":null},"body":{"tr_key":["J 005930"]}}`, 오류·제어 프레임은 header/body 가 null 이거나
 *   `rsp_cd`/`rsp_msg` 를 어느 쪽에든 싣는다(`""`/`"0"`/`"00000"` 가 정상). 데이터 프레임은 `header.tr_cd` 로 라우팅하고
 *   `header.tr_key` 는 항상 null 이라 심볼은 body(`ShrnIscd`/`Sshtnisuno`)에서 읽는다
 * - body 값은 모두 문자열(계좌계는 0 패딩). **필드명 대소문자가 문서와 예시에서 다르다**(`askp1` vs `Askp1`) → 대소문자 무시 조회
 *
 * 필드: S00 `BsopDate`·`StckCntghour`(HHmmss)·`StckPrpr`·`CntgVol`·`AcmlVol`·`PrdyVrss`(+`PrdyVrssclr` 부호)·`PrdyCtrt`(%)·`askp1`/`bidp1`,
 * S01 `BsopHour`·`askp1..10`/`bidp1..10`·`AskpRsqn1..10`/`BidpRsqn1..10`·`TotalAskprsqn`/`TotalBidprsqn`,
 * IS0 `Sordno`/`Sorgordno`/`Sshtnisuno`(A접두)/`Sbnstp`(1 매도 2 매수)/`Sordqty`/`Sordprc`/`Sordtm`(HHmmssSSS),
 * IS1 `Sexecqty`/`Sexecprc`/`Sunercqty`/`Scanccnfqty`/`Smdfycnfqty`/`Smdfycnfprc`/`Srjtqty`/`Sexectime`.
 * `Sordxctptncode`(주문체결유형코드) 값표는 미공개라 상태는 수량 필드로 판정한다. 픽스처: `conformance/fixtures/db.json#stream`.
 */
class DbMarketStream(
    private val properties: DbApiProperties,
    private val objectMapper: ObjectMapper,
    private val token: () -> String,
) : ReconnectingWebSocket("db", idleTimeoutMillis = 0), MarketStream {

    private val logger = KotlinLogging.logger { }

    private val tradeListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<TradeListener>>()
    private val bookListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<OrderBookListener>>()
    private val orderListeners = CopyOnWriteArrayList<OrderEventListener>()
    private val requestedSymbols = ConcurrentHashMap<String, String>()

    override val isConnected: Boolean get() = isSocketOpen

    override fun uri(): URI = URI.create(properties.resolvedWsUrl())

    /** 접속 직후 전부 다시 보낸다 — 10초 규칙 때문에 지체하지 않는다 */
    override fun onOpen() {
        val t = token()
        tradeListeners.keys.forEach { send(quoteMessage(t, TR_TRADE, it, "1")) }
        bookListeners.keys.forEach { send(quoteMessage(t, TR_ORDER_BOOK, it, "1")) }
        if (orderListeners.isNotEmpty()) sendAccountRegistrations(t)
    }

    override fun subscribeTrades(symbols: List<String>, listener: TradeListener) {
        val newCodes = register(symbols, listener, tradeListeners)
        if (newCodes.isNotEmpty() && isSocketOpen) {
            val t = token()
            newCodes.forEach { send(quoteMessage(t, TR_TRADE, it, "1")) }
        }
    }

    override fun subscribeOrderBook(symbols: List<String>, listener: OrderBookListener) {
        val newCodes = register(symbols, listener, bookListeners)
        if (newCodes.isNotEmpty() && isSocketOpen) {
            val t = token()
            newCodes.forEach { send(quoteMessage(t, TR_ORDER_BOOK, it, "1")) }
        }
    }

    override fun subscribeOrderEvents(listener: OrderEventListener) {
        val first = orderListeners.isEmpty()
        orderListeners += listener
        if (first && isSocketOpen) sendAccountRegistrations(token())
    }

    private fun sendAccountRegistrations(t: String) {
        send(accountMessage(t, TR_ORDER_ACCEPTED))
        send(accountMessage(t, TR_ORDER_EXECUTED))
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
        val body = node.path("body")
        // 제어 프레임: rsp_cd/rsp_msg 가 header 또는 body 에 실린다. header/body 둘 다 비면 keepalive 류 — 무시
        val rspCd = header.path("rsp_cd").asText(body.path("rsp_cd").asText(""))
        val rspMsg = header.path("rsp_msg").asText(body.path("rsp_msg").asText(""))
        if (rspCd.isNotBlank() || rspMsg.isNotBlank()) {
            if (rspCd in SUCCESS_CODES) logger.info { "db stream: ${header.path("tr_cd").asText("")} $rspMsg" }
            else logger.warn { "db stream: ${header.path("tr_cd").asText("")} rsp_cd=$rspCd $rspMsg" }
            return
        }
        if (header.isMissingNode || header.isNull || body.isMissingNode || body.isNull) return
        val trCd = header.path("tr_cd").asText("")
        if (body.path("tr_key").isArray) { // 구독 ack
            logger.info { "db stream: subscribed $trCd ${body.path("tr_key")}" }
            return
        }
        when (trCd) {
            TR_TRADE -> parseTrade(body)?.let { tick ->
                val symbol = requestedSymbols[tick.symbol] ?: tick.symbol
                val out = if (symbol == tick.symbol) tick else tick.copy(symbol = symbol)
                tradeListeners[tick.symbol]?.forEach { l -> runCatching { l.onTrade(out) }.onFailure { logger.error(it) { "db stream: 리스너 오류 / $symbol" } } }
            }
            TR_ORDER_BOOK -> parseOrderBook(body)?.let { tick ->
                val symbol = requestedSymbols[tick.symbol] ?: tick.symbol
                val out = if (symbol == tick.symbol) tick else tick.copy(symbol = symbol)
                bookListeners[tick.symbol]?.forEach { l -> runCatching { l.onOrderBook(out) }.onFailure { logger.error(it) { "db stream: 호가 리스너 오류 / $symbol" } } }
            }
            TR_ORDER_ACCEPTED, TR_ORDER_EXECUTED -> parseOrderEvent(trCd, body)?.let { event ->
                orderListeners.forEach { l -> runCatching { l.onOrderEvent(event) }.onFailure { logger.error(it) { "db stream: 주문 통보 리스너 오류 / ${event.orderId}" } } }
            }
            else -> logger.debug { "db stream: unknown tr $trCd" }
        }
    }

    private fun quoteMessage(t: String, trCd: String, code: String, trType: String): String =
        objectMapper.writeValueAsString(
            mapOf("header" to mapOf("token" to t, "tr_type" to trType), "body" to mapOf("tr_cd" to trCd, "tr_key" to MARKET_PREFIX + code)),
        )

    private fun accountMessage(t: String, trCd: String): String =
        objectMapper.writeValueAsString(mapOf("header" to mapOf("token" to t, "tr_type" to "3"), "body" to mapOf("tr_cd" to trCd)))

    companion object {
        const val TR_TRADE = "S00"
        const val TR_ORDER_BOOK = "S01"
        const val TR_ORDER_ACCEPTED = "IS0"
        const val TR_ORDER_EXECUTED = "IS1"

        /** KRX 주식/ETF 시장구분 2자리 — `J` + 공백. NXT 는 `NJN-`, 통합은 `UJU-` (미지원) */
        const val MARKET_PREFIX = "J "
        private val SUCCESS_CODES = setOf("", "0", "00000")
        private val HHMMSS: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmmss")

        /** 문서 표(`askp1`)와 예시(`Askp1`)의 대소문자가 달라 소문자 키로 조회한다 */
        private fun JsonNode.lower(): Map<String, JsonNode> = fields().asSequence().associate { (k, v) -> k.lowercase() to v }

        private fun Map<String, JsonNode>.text(field: String): String = this[field.lowercase()]?.asText("")?.trim() ?: ""

        private fun Map<String, JsonNode>.decimal(field: String): BigDecimal? =
            text(field).replace(",", "").takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

        private fun kstInstant(hhmmss: String, today: LocalDate): Instant =
            today.atTime(LocalTime.parse(hhmmss.take(6).padStart(6, '0'), HHMMSS)).atZone(KrxCalendar.KST).toInstant()

        /** `U-005930` / `N-005930` / `A005930` → `005930` */
        fun normalizeCode(raw: String): String = raw.trim().removePrefix("U-").removePrefix("N-").removePrefix("A")

        /** S00 body → 체결. 심볼은 단축코드(요청 표기 복원은 스트림이 한다). 필드 부족이면 null */
        fun parseTrade(body: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): TradeTick? = runCatching {
            val f = body.lower()
            val rawChange = f.decimal("PrdyVrss")
            val falling = f.text("PrdyVrssclr") == "-" || f.text("PrdyVrsssign") in setOf("4", "5")
            val change = rawChange?.let { if (falling && it.signum() > 0) it.negate() else it }
            TradeTick(
                symbol = normalizeCode(f.text("ShrnIscd")),
                price = f.decimal("StckPrpr") ?: error("no price"),
                quantity = f.decimal("CntgVol") ?: BigDecimal.ZERO,
                timestamp = kstInstant(f.text("StckCntghour"), today),
                askPrice = f.decimal("askp1"),
                bidPrice = f.decimal("bidp1"),
                cumulativeVolume = f.decimal("AcmlVol")?.toLong(),
                change = change,
                changeRate = f.decimal("PrdyCtrt")?.divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN),
            )
        }.getOrNull()

        /** S01 body → 호가창(10단계, 0 호가는 제외) */
        fun parseOrderBook(body: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): OrderBookTick? = runCatching {
            val f = body.lower()
            fun levels(priceField: String, qtyField: String): List<OrderBookLevel> =
                (1..10).mapNotNull { i ->
                    val price = f.decimal("$priceField$i") ?: return@mapNotNull null
                    if (price.signum() == 0) return@mapNotNull null
                    OrderBookLevel(price, f.decimal("$qtyField$i") ?: BigDecimal.ZERO)
                }
            OrderBookTick(
                symbol = normalizeCode(f.text("ShrnIscd")),
                timestamp = kstInstant(f.text("BsopHour"), today),
                asks = levels("askp", "AskpRsqn"),
                bids = levels("bidp", "BidpRsqn"),
                totalAskQuantity = f.decimal("TotalAskprsqn"),
                totalBidQuantity = f.decimal("TotalBidprsqn"),
            )
        }.getOrNull()

        /**
         * IS0(접수) / IS1(체결·정정·취소·거부) body → 주문 통보. 유형코드 값표가 없어 수량 필드로 판정한다:
         * 거부수량 > 0 → REJECTED, 체결수량 > 0 → FILLED, 취소확인수량 > 0 → CANCELED, 정정확인수량 > 0 → MODIFIED, 그 외 ACCEPTED
         */
        fun parseOrderEvent(trCd: String, body: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): OrderEvent? = runCatching {
            val f = body.lower()
            val orderId = f.text("Sordno").ifBlank { return@runCatching null }
            val original = f.text("Sorgordno").takeIf { it.isNotBlank() && it.trimStart('0').isNotEmpty() && it.trimStart('0') != orderId.trimStart('0') }
            val side = when (f.text("Sbnstp")) { "1" -> OrderSide.SELL; "2" -> OrderSide.BUY; else -> null }
            val symbol = normalizeCode(f.text("Sshtnisuno")).ifBlank { null }
            if (trCd == TR_ORDER_ACCEPTED) {
                return@runCatching OrderEvent(
                    orderId = orderId, type = OrderEventType.ACCEPTED, timestamp = kstInstant(f.text("Sordtm"), today),
                    symbol = symbol, side = side, quantity = f.decimal("Sordqty"), price = f.decimal("Sordprc"), originalOrderId = original,
                )
            }
            val rejected = f.decimal("Srjtqty")?.signum() ?: 0
            val executed = f.decimal("Sexecqty")?.signum() ?: 0
            val canceled = f.decimal("Scanccnfqty")?.signum() ?: 0
            val modified = f.decimal("Smdfycnfqty")?.signum() ?: 0
            val type = when {
                rejected > 0 -> OrderEventType.REJECTED
                executed > 0 -> OrderEventType.FILLED
                canceled > 0 -> OrderEventType.CANCELED
                modified > 0 -> OrderEventType.MODIFIED
                else -> OrderEventType.ACCEPTED
            }
            val (quantity, price) = when (type) {
                OrderEventType.FILLED -> f.decimal("Sexecqty") to f.decimal("Sexecprc")
                OrderEventType.MODIFIED -> f.decimal("Smdfycnfqty") to f.decimal("Smdfycnfprc")
                OrderEventType.CANCELED -> f.decimal("Scanccnfqty") to f.decimal("Sordprc")
                OrderEventType.REJECTED -> f.decimal("Srjtqty") to f.decimal("Sordprc")
                else -> f.decimal("Sordqty") to f.decimal("Sordprc")
            }
            OrderEvent(
                orderId = orderId, type = type, timestamp = kstInstant(f.text("Sexectime").ifBlank { f.text("Sordtm") }, today),
                symbol = symbol, side = side, quantity = quantity, price = price,
                remainingQuantity = f.decimal("Sunercqty"), originalOrderId = original,
            )
        }.getOrNull()
    }
}
