package com.tripleauth.hermetix.client.toss

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.BrokerUsage
import com.tripleauth.hermetix.broker.MarketStream
import com.tripleauth.hermetix.broker.MarketSymbol
import com.tripleauth.hermetix.broker.OrderBookLevel
import com.tripleauth.hermetix.broker.OrderBookListener
import com.tripleauth.hermetix.broker.OrderBookTick
import com.tripleauth.hermetix.broker.OrderEvent
import com.tripleauth.hermetix.broker.OrderEventListener
import com.tripleauth.hermetix.broker.OrderEventType
import com.tripleauth.hermetix.broker.ReconnectingWebSocket
import com.tripleauth.hermetix.broker.StreamChannel
import com.tripleauth.hermetix.broker.TradeListener
import com.tripleauth.hermetix.broker.TradeTick
import com.tripleauth.hermetix.client.dto.OrderSide
import io.github.oshai.kotlinlogging.KotlinLogging
import java.math.BigDecimal
import java.net.URI
import java.time.Instant
import java.time.OffsetDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * 토스증권 실시간 스트림. 공식 AsyncAPI 1.2.2 문서 기반, **실측 전** (모의투자 서버가 없어 실계좌로만 검증 가능).
 *
 * 프로토콜 (`wss://openapi-ws.tossinvest.com/ws/v1`, `openapi.tossinvest.com/openapi-docs/latest/asyncapi.json`):
 * - 핸드셰이크에 `Authorization: Bearer {access_token}` — REST 와 같은 토큰. 토큰은 접속 때만 검사된다
 * - 구독은 **선언형**: 클라이언트가 보내는 JSON 배열 하나가 현재 구독 집합 전체다. 새 배열이 이전 집합을 통째로 대체하고, 빠진 항목은
 *   자동 해제된다. 요소는 `{"type":"trade:kr","codes":["005930"]}` 꼴, 첫 요소로 `{"id":"req-N"}` 을 넣으면 응답에 echo 된다.
 *   type: `trade:us` / `trade:kr` / `orderbook:us` / `orderbook:kr` (codes = 종목코드, 미국은 대문자 티커), `personal:order` (codes = accountSeq 문자열)
 * - 서버 프레임은 `type` 으로 구분: `subscriptions`(구독 결과, `subscribed[]`·`rejected[]{target,code,message}`), `message`(데이터,
 *   `topic` = `trade:kr:005930` 처럼 `{type}:{code}`), `error`(선언 전체 실패·서버 재시작 `server-shutdown`), `pong`
 * - 180초 동안 클라이언트가 아무것도 보내지 않으면 서버가 끊는다 → 60초마다 텍스트 프레임 `PING` (JSON 아님) → `{"type":"pong"}`
 * - 한도: 계정당 연결 2개, 연결당 구독 100개, 선언 5회/초 → 구독 변경은 [declareDelayMillis] 동안 모아 한 번에 보낸다
 * - trade/orderbook 은 유실 가능(백프레셔 시 최신 우선), personal:order 는 세션 내 무손실. 재접속 뒤 놓친 이벤트는 재전송되지 않는다
 *
 * 프레임 (모든 숫자는 문자열):
 * - trade `data`: `price`, `volume`(이 체결 수량), `timestamp`(ISO-8601 +09:00), `currency`. **누적거래량·등락·매수매도 구분 없음**
 * - orderbook `data`: `timestamp`(null 가능), `currency`, `asks[]`/`bids[]` `{price, volume}` (최우선부터, 단계 수 미명시)
 * - personal:order `data`: `event` (PENDING / PARTIAL_FILL / FILL / CANCELING / CANCELED / REPLACING / REPLACED / REJECTED / CANCEL_REJECTED /
 *   REPLACE_REJECTED), `accountSeq`, `order` (REST 주문 상세와 같은 스냅샷 — `execution.filledQuantity` 는 **누적**이라 이번 체결량은 직전 스냅샷과의 차이)
 *
 * 문서로 확정하지 못한 점(실측 필요): 호가 단계 수, 취소·정정 시 어떤 orderId 로 이벤트가 오는지(취소는 REST 에서 새 orderId 를 발급한다), 표준 ping 프레임이 유휴 타이머를 리셋하는지.
 */
class TossMarketStream(
    private val properties: TossApiProperties,
    private val objectMapper: ObjectMapper,
    private val token: () -> String,
    private val accountSeq: () -> String,
    heartbeatMillis: Long = 60_000,
    private val declareDelayMillis: Long = 200,
    usage: BrokerUsage? = null,
) : ReconnectingWebSocket("toss", idleTimeoutMillis = 0, heartbeatMillis = heartbeatMillis, usage = usage), MarketStream {

    private val logger = KotlinLogging.logger { }

    /** topic 키(`kr:005930`) → 리스너 */
    private val tradeListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<TradeListener>>()
    private val bookListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<OrderBookListener>>()
    private val orderListeners = CopyOnWriteArrayList<OrderEventListener>()

    /** topic 키 → 구독 요청 표기 (`KRX:005930` 로 구독하면 그대로 돌려준다) */
    private val requestedSymbols = ConcurrentHashMap<String, String>()

    /** 서버가 거부한 target(`trade:kr:999999`) — 다시 선언하면 또 거부되므로 뺀다 */
    private val rejectedTargets: MutableSet<String> = ConcurrentHashMap.newKeySet()

    /** orderId → 직전 스냅샷의 누적 체결량 (이번 체결량 = 차이) */
    private val filledSoFar = ConcurrentHashMap<String, BigDecimal>()

    private val declareExecutor = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "toss-ws-declare").apply { isDaemon = true } }

    @Volatile
    private var pendingDeclare: ScheduledFuture<*>? = null

    private val requestCounter = AtomicInteger()

    override val isConnected: Boolean get() = isSocketOpen

    override fun uri(): URI = URI.create(properties.wsUrl)

    override fun headers(): Map<String, String> = mapOf("Authorization" to "Bearer ${token()}")

    override fun onOpen() {
        declareNow()
    }

    override fun onHeartbeat() {
        send("PING")
    }

    override fun close() {
        pendingDeclare?.cancel(false)
        declareExecutor.shutdownNow()
        super.close()
    }

    // ------------------------------------------------------------------ subscribe

    override fun subscribeTrades(symbols: List<String>, listener: TradeListener) {
        val added = register(symbols, listener, tradeListeners)
        usage?.streamSubscribed(StreamChannel.TRADES, added)
        if (added > 0) scheduleDeclare()
    }

    override fun subscribeOrderBook(symbols: List<String>, listener: OrderBookListener) {
        val added = register(symbols, listener, bookListeners)
        usage?.streamSubscribed(StreamChannel.ORDER_BOOK, added)
        if (added > 0) scheduleDeclare()
    }

    override fun subscribeOrderEvents(listener: OrderEventListener) {
        val first = orderListeners.isEmpty()
        orderListeners += listener
        if (first) usage?.streamSubscribed(StreamChannel.ORDER_EVENTS)
        if (first) scheduleDeclare()
    }

    /** 새로 생긴 topic 수 — 0 보다 크면 선언을 다시 보내야 한다 */
    private fun <L> register(symbols: List<String>, listener: L, target: ConcurrentHashMap<String, CopyOnWriteArrayList<L>>): Int {
        var added = 0
        symbols.forEach { symbol ->
            val key = topicKey(symbol)
            requestedSymbols.putIfAbsent(key, symbol)
            target.computeIfAbsent(key) { added++; CopyOnWriteArrayList() } += listener
        }
        return added
    }

    private fun scheduleDeclare() {
        if (!isSocketOpen) return // 접속되면 onOpen 이 전체를 선언한다
        pendingDeclare?.cancel(false)
        pendingDeclare = runCatching {
            declareExecutor.schedule({ declareNow() }, declareDelayMillis, TimeUnit.MILLISECONDS)
        }.getOrNull()
    }

    /** 현재 구독 집합 전체를 한 배열로 보낸다 */
    private fun declareNow() {
        val declaration = buildDeclaration()
        if (declaration.size <= 1) return // id 만 있으면 보낼 게 없다
        send(objectMapper.writeValueAsString(declaration))
    }

    internal fun buildDeclaration(): List<Map<String, Any>> {
        val items = mutableListOf<Map<String, Any>>(mapOf("id" to "req-${requestCounter.incrementAndGet()}"))
        fun codesFor(prefix: String, keys: Set<String>, market: String): List<String> =
            keys.filter { it.startsWith("$market:") }
                .map { it.substringAfter(':') }
                .filter { "$prefix:$market:$it" !in rejectedTargets }
                .sorted()
        for (market in listOf("kr", "us")) {
            codesFor("trade", tradeListeners.keys, market).takeIf { it.isNotEmpty() }?.let { items += mapOf("type" to "trade:$market", "codes" to it) }
        }
        for (market in listOf("kr", "us")) {
            codesFor("orderbook", bookListeners.keys, market).takeIf { it.isNotEmpty() }?.let { items += mapOf("type" to "orderbook:$market", "codes" to it) }
        }
        if (orderListeners.isNotEmpty()) {
            val seq = runCatching { accountSeq() }.onFailure { logger.warn { "toss stream: accountSeq 조회 실패 - 주문 이벤트 구독 보류: ${it.message}" } }.getOrNull()
            if (seq != null && "personal:order:$seq" !in rejectedTargets) items += mapOf("type" to "personal:order", "codes" to listOf(seq))
        }
        return items
    }

    // ------------------------------------------------------------------ frames

    override fun onMessage(text: String) {
        val node = runCatching { objectMapper.readTree(text) }.getOrNull() ?: return
        when (node.path("type").asText("")) {
            "pong" -> Unit
            "subscriptions" -> onSubscriptions(node)
            "error" -> {
                val error = node.path("error")
                logger.warn { "toss stream: error ${error.path("code").asText("")} ${error.path("message").asText("")} (id=${node.path("id").asText("")})" }
            }
            "message" -> onData(node.path("topic").asText(""), node.path("data"))
            else -> logger.debug { "toss stream: ${text.take(200)}" }
        }
    }

    private fun onSubscriptions(node: JsonNode) {
        val subscribed = node.path("subscribed").map { it.asText() }
        val rejected = node.path("rejected")
        logger.info { "toss stream: subscribed ${subscribed.size} (id=${node.path("id").asText("")})" }
        rejected.forEach { r ->
            val target = r.path("target").asText("")
            logger.warn { "toss stream: 구독 거부 $target ${r.path("code").asText("")} ${r.path("message").asText("")} — 선언에서 제외한다" }
            if (target.isNotBlank()) rejectedTargets += target
        }
    }

    private fun onData(topic: String, data: JsonNode) {
        val parts = topic.split(":", limit = 3)
        if (parts.size < 3) return
        when (parts[0]) {
            "trade" -> parseTrade(topic, data)?.let { tick ->
                val key = "${parts[1]}:${parts[2]}"
                val out = requestedSymbols[key]?.let { tick.copy(symbol = it) } ?: tick
                usage?.streamMessage(StreamChannel.TRADES)
                tradeListeners[key]?.forEach { l -> runCatching { l.onTrade(out) }.onFailure { logger.error(it) { "toss stream: 리스너 오류 / ${out.symbol}" } } }
            }
            "orderbook" -> parseOrderBook(topic, data)?.let { book ->
                val key = "${parts[1]}:${parts[2]}"
                val out = requestedSymbols[key]?.let { book.copy(symbol = it) } ?: book
                usage?.streamMessage(StreamChannel.ORDER_BOOK)
                bookListeners[key]?.forEach { l -> runCatching { l.onOrderBook(out) }.onFailure { logger.error(it) { "toss stream: 호가 리스너 오류 / ${out.symbol}" } } }
            }
            "personal" -> {
                val orderId = data.path("order").path("orderId").asText("")
                val event = parseOrderEvent(data, filledSoFar[orderId]) ?: return
                data.path("order").path("execution").path("filledQuantity").asText("").toBigDecimalOrNull()?.let { filledSoFar[orderId] = it }
                if (event.type == OrderEventType.CANCELED || event.type == OrderEventType.REJECTED) filledSoFar.remove(orderId)
                usage?.streamMessage(StreamChannel.ORDER_EVENTS)
                orderListeners.forEach { l -> runCatching { l.onOrderEvent(event) }.onFailure { logger.error(it) { "toss stream: 주문 이벤트 리스너 오류 / ${event.orderId}" } } }
            }
        }
    }

    companion object {
        /** Hermetix 심볼 → topic 키 (`KRX:005930`/`005930` → `kr:005930`, `US:AAPL` → `us:AAPL`) */
        fun topicKey(symbol: String): String {
            val parsed = MarketSymbol.parse(symbol)
            val market = when (parsed.market) {
                null, "KRX" -> "kr"
                "US" -> "us"
                else -> throw IllegalArgumentException("토스 어댑터가 지원하지 않는 시장: ${parsed.market} ($symbol)")
            }
            val code = if (market == "us") parsed.code.uppercase() else parsed.code
            return "$market:$code"
        }

        /** topic 의 시장·코드 → 정규 표기 (`KRX:005930`, `US:AAPL`) */
        fun canonicalSymbol(market: String, code: String): String = if (market == "us") "US:$code" else "KRX:$code"

        private fun JsonNode.decimalOrNull(field: String): BigDecimal? =
            path(field).takeIf { !it.isMissingNode && !it.isNull }?.asText("")?.trim()?.takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

        private fun JsonNode.instantOrNull(field: String): Instant? =
            path(field).takeIf { !it.isMissingNode && !it.isNull }?.asText("")?.takeIf { it.isNotBlank() }
                ?.let { runCatching { OffsetDateTime.parse(it).toInstant() }.getOrNull() }

        /** `trade:{kr|us}:{code}` 데이터 → 체결. 누적거래량·등락·호가는 프레임에 없어 null */
        fun parseTrade(topic: String, data: JsonNode): TradeTick? {
            val parts = topic.split(":", limit = 3)
            if (parts.size < 3 || parts[0] != "trade") return null
            return runCatching {
                TradeTick(
                    symbol = canonicalSymbol(parts[1], parts[2]),
                    price = data.decimalOrNull("price") ?: error("no price"),
                    quantity = data.decimalOrNull("volume") ?: BigDecimal.ZERO,
                    timestamp = data.instantOrNull("timestamp") ?: Instant.now(),
                )
            }.getOrNull()
        }

        /** `orderbook:{kr|us}:{code}` 데이터 → 호가창. asks 오름차순·bids 내림차순으로 오므로 순서 그대로가 최우선부터 */
        fun parseOrderBook(topic: String, data: JsonNode): OrderBookTick? {
            val parts = topic.split(":", limit = 3)
            if (parts.size < 3 || parts[0] != "orderbook") return null
            fun levels(node: JsonNode): List<OrderBookLevel> = node.mapNotNull { l ->
                val price = l.decimalOrNull("price") ?: return@mapNotNull null
                OrderBookLevel(price, l.decimalOrNull("volume") ?: BigDecimal.ZERO)
            }
            return OrderBookTick(
                symbol = canonicalSymbol(parts[1], parts[2]),
                timestamp = data.instantOrNull("timestamp") ?: Instant.now(),
                asks = levels(data.path("asks")),
                bids = levels(data.path("bids")),
            )
        }

        /**
         * `personal:order` 데이터 → 주문 이벤트. [previousFilled] 는 같은 orderId 의 직전 누적 체결량 (없으면 이번이 첫 스냅샷).
         * CANCELING/REPLACING 은 중간 상태라 null.
         */
        fun parseOrderEvent(data: JsonNode, previousFilled: BigDecimal?): OrderEvent? {
            val order = data.path("order")
            val orderId = order.path("orderId").asText("").ifBlank { return null }
            val execution = order.path("execution")
            val filled = execution.decimalOrNull("filledQuantity") ?: BigDecimal.ZERO
            val quantity = order.decimalOrNull("quantity")
            val price = order.decimalOrNull("price")
            val eventName = data.path("event").asText("")
            val type = when (eventName) {
                "PENDING" -> OrderEventType.ACCEPTED
                "PARTIAL_FILL", "FILL" -> OrderEventType.FILLED
                "CANCELED" -> OrderEventType.CANCELED
                "REPLACED" -> OrderEventType.MODIFIED
                "REJECTED", "CANCEL_REJECTED", "REPLACE_REJECTED" -> OrderEventType.REJECTED
                else -> return null // CANCELING, REPLACING, 미지 값
            }
            val market = if (order.path("currency").asText("") == "USD") "US" else "KRX"
            val symbol = order.path("symbol").asText("").takeIf { it.isNotBlank() }?.let { "$market:$it" }
            val fillQuantity = (filled - (previousFilled ?: BigDecimal.ZERO)).let { if (it.signum() < 0) filled else it }
            return OrderEvent(
                orderId = orderId,
                type = type,
                timestamp = order.instantOrNull("orderedAt") ?: Instant.now(),
                symbol = symbol,
                side = when (order.path("side").asText("")) { "BUY" -> OrderSide.BUY; "SELL" -> OrderSide.SELL; else -> null },
                quantity = if (type == OrderEventType.FILLED) fillQuantity else quantity,
                price = if (type == OrderEventType.FILLED) (execution.decimalOrNull("averageFilledPrice") ?: price) else price,
                remainingQuantity = quantity?.let { it - filled },
                reason = if (type == OrderEventType.REJECTED) eventName else null,
            )
        }
    }
}
