package com.tripleauth.hermetix.client.nh

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
 * NH PLUG 실시간 스트림. **문서 기반 구현, 실측 전** — 포털 `openapi.json` `x-realtime-channels`·API 가이드 DB·공식 Python SDK(`nhplug-sdk`) 에서 역추적.
 *
 * 프로토콜:
 * - 접속: 모의 `wss://moapi.nhplug.com:17070/websocket`, 운영 `wss://api.nhplug.com:7070/websocket` (경로 `/websocket` 필수). 핸드셰이크 헤더 없음
 * - 인증: 별도 로그인 프레임 없이 **매 구독 메시지의 `header.token`** (REST 접근토큰 그대로)
 * - 구독: `{"header":{"token","tr_type":"1"|"2"},"body":{"tr_cd":<채널>,"tr_key":<종목코드>}}`. 통보 채널은 `tr_key` 빈 문자열
 * - 채널: 체결 KRX `oc` / NXT `nc` / 통합 `mc`, 호가 `ob` / `nb` / `mb` — REST 와 달리 시장을 채널코드로 고른다 ([NhApiProperties.marketCd] 에 맞춤).
 *   통보는 체결 `d2`(체결·정정·취소·거부 결과) + 접수 `d3`(신규·정정·취소 접수), 둘 다 국내주식/국내파생 공용(`itemgb` 로 구분)
 * - 응답(ACK): `{"header":{"tr_type","tr_cd","rsp_cd":"00000","rsp_msg"},"body":{"tr_key":[…]}}` — 데이터 푸시 header 에는 `rsp_cd`/`tr_type` 이 없다
 * - 데이터: `{"header":{"tr_cd","tr_key"},"body":{…}}` (통보는 header 에 `tr_key` 없음). 값은 예시상 전부 문자열 — 숫자도 허용해 파싱
 * - heartbeat 없음(문서 명시) — 조용한 게 정상이므로 유휴 감시를 끈다. 암호화 없음 (KIS 식 AES 없음)
 *
 * 문서로 확정하지 못한 점 (실측 필요):
 * - `sign`/`kospigb`/`janggubun`/`ordercd`/`order_type`/`procnm` 코드값 — `sign` 은 REST `prdy_vrss_sign` 과 같은 체계(4/5/8/9 하락)로 가정
 * - `volume` vs `new_volume`(누적거래량) — 예시에서 같은 값. `movolume`(변동거래량)을 이번 체결 수량으로 해석
 * - `value` 는 백만원 단위, `value_won` 이 원 단위 (예시 비율로 추정)
 * - `orderno`(d2/d3) 와 REST `mkt_orr_no` 의 동일성 → 선행 0 을 무시하고 비교([OrderEvent.orderIdMatches])
 * - 거부가 `d3` 없이 `d2(rejgb=1)` 로만 오는지, 정정이 `d3` 접수 → `d2 ucgb=1` 순서인지
 * - 시간 필드에 날짜·타임존이 없어 오늘(KST)로 붙인다. `market_chrate` 는 포털 표에 `markeet_chrate` 오타가 있어 둘 다 허용
 * - 모의(17070)에서 시세 채널이 오는지 (포털 가이드는 "미제공")
 */
class NhMarketStream(
    private val properties: NhApiProperties,
    private val objectMapper: ObjectMapper,
    private val token: () -> String,
) : ReconnectingWebSocket("nh", idleTimeoutMillis = 0), MarketStream {

    private val logger = KotlinLogging.logger { }

    private val tradeListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<TradeListener>>()
    private val bookListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<OrderBookListener>>()
    private val orderListeners = CopyOnWriteArrayList<OrderEventListener>()
    private val requestedSymbols = ConcurrentHashMap<String, String>()

    /** 시장 구분에 따른 채널코드 — KRX oc/ob, NXT nc/nb, UNT(통합) mc/mb. tr_cd 는 대소문자를 구분하므로 정규화하지 않는다 */
    internal val tradeChannel: String = when (properties.marketCd.uppercase()) { "NXT" -> "nc"; "UNT" -> "mc"; else -> "oc" }
    internal val bookChannel: String = when (properties.marketCd.uppercase()) { "NXT" -> "nb"; "UNT" -> "mb"; else -> "ob" }

    override val isConnected: Boolean get() = isSocketOpen

    override fun uri(): URI = URI.create(properties.resolvedWsUrl())

    override fun onOpen() {
        val t = token()
        tradeListeners.keys.forEach { code -> send(message(t, "1", tradeChannel, code)) }
        bookListeners.keys.forEach { code -> send(message(t, "1", bookChannel, code)) }
        if (orderListeners.isNotEmpty()) ORDER_CHANNELS.forEach { send(message(t, "1", it, "")) }
    }

    override fun subscribeTrades(symbols: List<String>, listener: TradeListener) {
        val newCodes = register(symbols, listener, tradeListeners)
        if (newCodes.isNotEmpty() && isSocketOpen) { val t = token(); newCodes.forEach { send(message(t, "1", tradeChannel, it)) } }
    }

    override fun subscribeOrderBook(symbols: List<String>, listener: OrderBookListener) {
        val newCodes = register(symbols, listener, bookListeners)
        if (newCodes.isNotEmpty() && isSocketOpen) { val t = token(); newCodes.forEach { send(message(t, "1", bookChannel, it)) } }
    }

    override fun subscribeOrderEvents(listener: OrderEventListener) {
        val first = orderListeners.isEmpty()
        orderListeners += listener
        if (first && isSocketOpen) { val t = token(); ORDER_CHANNELS.forEach { send(message(t, "1", it, "")) } }
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
        if (header.has("rsp_cd") || header.has("tr_type")) {
            val rspCd = header.path("rsp_cd").asText("")
            val msg = header.path("rsp_msg").asText("")
            if (rspCd == "00000") logger.info { "nh stream: ${if (header.path("tr_type").asText() == "2") "unsubscribed" else "subscribed"} $trCd ${node.path("body").path("tr_key")} ($msg)" }
            else logger.warn { "nh stream: $trCd rsp_cd=$rspCd $msg" }
            return
        }
        val body = node.path("body")
        if (body.isMissingNode || body.isNull) return
        when (trCd) {
            "oc", "nc", "mc" -> parseTrade(node).forEach { tick ->
                val symbol = requestedSymbols[tick.symbol] ?: tick.symbol
                val out = if (symbol == tick.symbol) tick else tick.copy(symbol = symbol)
                tradeListeners[tick.symbol]?.forEach { l -> runCatching { l.onTrade(out) }.onFailure { logger.error(it) { "nh stream: 리스너 오류 / $symbol" } } }
            }
            "ob", "nb", "mb" -> parseOrderBook(node).forEach { tick ->
                val symbol = requestedSymbols[tick.symbol] ?: tick.symbol
                val out = if (symbol == tick.symbol) tick else tick.copy(symbol = symbol)
                bookListeners[tick.symbol]?.forEach { l -> runCatching { l.onOrderBook(out) }.onFailure { logger.error(it) { "nh stream: 호가 리스너 오류 / $symbol" } } }
            }
            "d2", "d3" -> parseOrderEvents(node, accountNo = properties.accountNo).forEach { event ->
                orderListeners.forEach { l -> runCatching { l.onOrderEvent(event) }.onFailure { logger.error(it) { "nh stream: 주문 통보 리스너 오류 / ${event.orderId}" } } }
            }
            else -> logger.debug { "nh stream: unknown tr_cd $trCd" }
        }
    }

    private fun message(token: String, trType: String, trCd: String, trKey: String): String =
        objectMapper.writeValueAsString(mapOf("header" to mapOf("token" to token, "tr_type" to trType), "body" to mapOf("tr_cd" to trCd, "tr_key" to trKey)))

    companion object {
        val ORDER_CHANNELS = listOf("d2", "d3")
        private val HHMMSS: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmmss")
        private val BOOK_PREFIXES = listOf("", "P_", "S_", "S4_", "S5_", "S6_", "S7_", "S8_", "S9_", "S10_")

        /** 문자열/숫자 어느 쪽으로 와도 파싱 */
        private fun JsonNode.dec(field: String): BigDecimal? =
            path(field).takeIf { !it.isMissingNode && !it.isNull }?.asText("")?.trim()?.replace(",", "")?.takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

        private fun JsonNode.text(field: String): String = path(field).asText("").trim()

        /** "HH:MM:SS" 또는 "HHMMSS" (KST, 날짜 없음 → today) */
        private fun kstInstant(raw: String, today: LocalDate): Instant {
            val digits = raw.filter { it.isDigit() }.padStart(6, '0').take(6)
            return today.atTime(LocalTime.parse(digits, HHMMSS)).atZone(KrxCalendar.KST).toInstant()
        }

        /** 체결 프레임(oc/nc/mc) → 체결. 심볼은 header.tr_key 또는 body.code 그대로 (요청 표기 복원은 스트림이 한다) */
        fun parseTrade(node: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): List<TradeTick> {
            val body = node.path("body")
            if (body.isMissingNode || body.isNull) return emptyList()
            return runCatching {
                val sign = body.text("sign")
                val change = body.dec("change")?.let { if (sign in NhApiClient.FALLING_SIGNS && it.signum() > 0) it.negate() else it }
                val rate = body.dec("chrate")?.let { if (sign in NhApiClient.FALLING_SIGNS && it.signum() > 0) it.negate() else it }
                listOf(
                    TradeTick(
                        symbol = node.path("header").text("tr_key").ifBlank { body.text("code") },
                        price = body.dec("price") ?: error("no price"),
                        quantity = body.dec("movolume") ?: BigDecimal.ZERO,
                        timestamp = kstInstant(body.text("time"), today),
                        askPrice = body.dec("offer"),
                        bidPrice = body.dec("bid"),
                        cumulativeVolume = (body.dec("new_volume") ?: body.dec("volume"))?.toLong(),
                        change = change,
                        changeRate = rate?.divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN),
                    ),
                )
            }.getOrDefault(emptyList())
        }

        /** 호가 프레임(ob/nb/mb) → 호가창. 1단계 접두 없음, 2단계 P_, 3단계 S_, 4~10단계 S4_~S10_. 0 호가는 뺀다 */
        fun parseOrderBook(node: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): List<OrderBookTick> {
            val body = node.path("body")
            if (body.isMissingNode || body.isNull) return emptyList()
            return runCatching {
                fun levels(priceField: String, qtyField: String): List<OrderBookLevel> =
                    BOOK_PREFIXES.mapNotNull { p ->
                        val price = body.dec("$p$priceField") ?: return@mapNotNull null
                        if (price.signum() == 0) return@mapNotNull null
                        OrderBookLevel(price, body.dec("$p$qtyField") ?: BigDecimal.ZERO)
                    }
                listOf(
                    OrderBookTick(
                        symbol = node.path("header").text("tr_key").ifBlank { body.text("code") },
                        timestamp = kstInstant(body.text("hotime"), today),
                        asks = levels("offer", "offerrem"),
                        bids = levels("bid", "bidrem"),
                        totalAskQuantity = body.dec("T_offerrem"),
                        totalBidQuantity = body.dec("T_bidrem"),
                    ),
                )
            }.getOrDefault(emptyList())
        }

        /**
         * 통보 프레임 → 이벤트. `d3`(접수) → ACCEPTED, `d2` → rejgb=1 REJECTED / ucgb 0 체결 FILLED, 1 정정 MODIFIED, 2 취소·3 효력해제 CANCELED.
         * 국내주식(`itemgb=1`)만, [accountNo] 가 주어지면 계좌가 같은 것만.
         */
        fun parseOrderEvents(node: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST), accountNo: String = ""): List<OrderEvent> {
            val trCd = node.path("header").text("tr_cd")
            val b = node.path("body")
            if (b.isMissingNode || b.isNull || trCd !in ORDER_CHANNELS) return emptyList()
            if (b.text("itemgb") != "1") return emptyList()
            if (accountNo.isNotBlank() && b.text("accountno").isNotBlank() && b.text("accountno") != accountNo) return emptyList()
            return runCatching {
                val side = when (b.text("slbygb")) { "1" -> OrderSide.SELL; "2" -> OrderSide.BUY; else -> null }
                val original = b.text("orgordno").takeIf { it.isNotBlank() && it.trimStart('0').isNotEmpty() }
                val event = if (trCd == "d3") {
                    OrderEvent(
                        orderId = b.text("orderno"), type = OrderEventType.ACCEPTED, timestamp = kstInstant(b.text("order_time"), today),
                        symbol = NhApiClient.normalizeCode(b.text("issuecd")), side = side,
                        quantity = b.dec("ordergty"), price = b.dec("orderprc"), originalOrderId = original,
                    )
                } else {
                    val type = when {
                        b.text("rejgb") == "1" -> OrderEventType.REJECTED
                        b.text("ucgb") == "1" -> OrderEventType.MODIFIED
                        b.text("ucgb") == "2" || b.text("ucgb") == "3" -> OrderEventType.CANCELED
                        else -> OrderEventType.FILLED
                    }
                    OrderEvent(
                        orderId = b.text("orderno"), type = type, timestamp = kstInstant(b.text("conctime"), today),
                        symbol = NhApiClient.normalizeCode(b.text("issuecd")), side = side,
                        quantity = b.dec("concgty"), price = b.dec("concprc"),
                    )
                }
                listOf(event)
            }.getOrDefault(emptyList())
        }
    }
}
