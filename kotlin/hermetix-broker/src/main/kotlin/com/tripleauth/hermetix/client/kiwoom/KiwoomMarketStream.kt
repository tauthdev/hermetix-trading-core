package com.tripleauth.hermetix.client.kiwoom

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.BrokerUsage
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
import com.tripleauth.hermetix.broker.StreamChannel
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
 * 키움 REST API 실시간 스트림. 체결 `0B`·호가 `0D` 는 2026-09-14 모의투자 서버(mockapi) 장중 실측 통과(00 등록도 return_code 0),
 * 주문체결 `00` 프레임은 문서 기반 (이 모의 계좌는 공매도 이수 전용이라 주문 불가 → 통보 실측 전).
 *
 * 프로토콜 (키움 REST API 가이드 — 실시간시세):
 * - 접속: 모의 `wss://mockapi.kiwoom.com:10000/api/dostk/websocket`, 실전 `wss://api.kiwoom.com:10000/…`
 * - 로그인: 접속 직후 `{"trnm":"LOGIN","token":<접근토큰>}` → `{"trnm":"LOGIN","return_code":0,"sor_yn":"Y"}`. REST 토큰을 그대로 쓴다
 * - 등록: `{"trnm":"REG","grp_no":"1","refresh":"1","data":[{"item":[코드…],"type":["0B"]}]}` → `{"trnm":"REG","return_code":0}`.
 *   주문체결(00)은 계좌 단위라 item 을 빈 문자열 하나로 등록한다
 * - 데이터: `{"data":[{"values":{…},"type":"0B","name":"주식체결","item":"005930"}],"trnm":"REAL"}` (실측: data 가 trnm 앞)
 * - `{"trnm":"PING"}` 은 받은 그대로 되돌려 보낸다
 *
 * FID:
 * - 0B 주식체결: 20 체결시각, 10 현재가, 11 전일대비, 12 등락율(%), 27 최우선매도호가, 28 최우선매수호가, 15 거래량(+매수/-매도), 13 누적거래량
 * - 0D 주식호가잔량: 21 호가시각, 41–50 매도호가1–10, 61–70 매도잔량1–10, 51–60 매수호가1–10, 71–80 매수잔량1–10, 121 총매도잔량, 125 총매수잔량
 * - 00 주문체결: 9203 주문번호, 904 원주문번호, 9001 종목코드(A접두), 913 주문상태(접수/체결/확인), 905 주문구분(+매수/-매도/매수취소…), 907 매도수구분(1 매도/2 매수),
 *   900 주문수량, 901 주문가격, 902 미체결수량, 910 체결가, 911 체결량, 908 주문/체결시각, 919 거부사유
 * REST 와 같이 가격·호가·수량에 등락 부호가 붙으므로 절대값으로 파싱한다. 실측 프레임은 `conformance/fixtures/kiwoom.json#stream`.
 */
class KiwoomMarketStream(
    private val properties: KiwoomApiProperties,
    private val objectMapper: ObjectMapper,
    private val token: () -> String,
    usage: BrokerUsage? = null,
) : ReconnectingWebSocket("kiwoom", usage = usage), MarketStream {

    private val logger = KotlinLogging.logger { }

    private val tradeListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<TradeListener>>()
    private val bookListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<OrderBookListener>>()
    private val orderListeners = CopyOnWriteArrayList<OrderEventListener>()
    private val requestedSymbols = ConcurrentHashMap<String, String>()

    @Volatile
    private var loggedIn = false

    override val isConnected: Boolean get() = isSocketOpen && loggedIn

    override fun uri(): URI = URI.create(properties.resolvedWsUrl())

    override fun onOpen() {
        loggedIn = false
        send(objectMapper.writeValueAsString(mapOf("trnm" to "LOGIN", "token" to token())))
    }

    override fun onDisconnected() {
        loggedIn = false
    }

    override fun subscribeTrades(symbols: List<String>, listener: TradeListener) {
        val newCodes = register(symbols, listener, tradeListeners)
        usage?.streamSubscribed(StreamChannel.TRADES, newCodes.size)
        if (newCodes.isNotEmpty() && isConnected) send(registerMessage(newCodes, TYPE_TRADE))
    }

    override fun subscribeOrderBook(symbols: List<String>, listener: OrderBookListener) {
        val newCodes = register(symbols, listener, bookListeners)
        usage?.streamSubscribed(StreamChannel.ORDER_BOOK, newCodes.size)
        if (newCodes.isNotEmpty() && isConnected) send(registerMessage(newCodes, TYPE_ORDER_BOOK))
    }

    override fun subscribeOrderEvents(listener: OrderEventListener) {
        val first = orderListeners.isEmpty()
        orderListeners += listener
        if (first) usage?.streamSubscribed(StreamChannel.ORDER_EVENTS)
        if (first && isConnected) send(registerMessage(listOf(""), TYPE_ORDER_EVENTS))
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

    private fun registerAll() {
        if (tradeListeners.isNotEmpty()) send(registerMessage(tradeListeners.keys.toList(), TYPE_TRADE))
        if (bookListeners.isNotEmpty()) send(registerMessage(bookListeners.keys.toList(), TYPE_ORDER_BOOK))
        if (orderListeners.isNotEmpty()) send(registerMessage(listOf(""), TYPE_ORDER_EVENTS))
    }

    override fun onMessage(text: String) {
        val node = runCatching { objectMapper.readTree(text) }.getOrNull() ?: return
        when (node.path("trnm").asText("")) {
            "PING" -> send(text)
            "LOGIN" -> {
                val code = node.path("return_code").asInt(-1)
                if (code == 0) {
                    loggedIn = true
                    logger.info { "kiwoom stream: logged in" }
                    registerAll()
                } else {
                    logger.error { "kiwoom stream: 로그인 실패 return_code=$code ${node.path("return_msg").asText("")}" }
                }
            }
            "REG" -> {
                val code = node.path("return_code").asInt(-1)
                if (code == 0) logger.info { "kiwoom stream: registered" }
                else logger.warn { "kiwoom stream: 등록 실패 return_code=$code ${node.path("return_msg").asText("")}" }
            }
            "REAL" -> {
                parseReal(node).forEach { tick ->
                    val symbol = requestedSymbols[tick.symbol] ?: tick.symbol
                    val out = if (symbol == tick.symbol) tick else tick.copy(symbol = symbol)
                    usage?.streamMessage(StreamChannel.TRADES)
                    tradeListeners[tick.symbol]?.forEach { l -> runCatching { l.onTrade(out) }.onFailure { logger.error(it) { "kiwoom stream: 리스너 오류 / $symbol" } } }
                }
                parseOrderBook(node).forEach { tick ->
                    val symbol = requestedSymbols[tick.symbol] ?: tick.symbol
                    val out = if (symbol == tick.symbol) tick else tick.copy(symbol = symbol)
                    usage?.streamMessage(StreamChannel.ORDER_BOOK)
                    bookListeners[tick.symbol]?.forEach { l -> runCatching { l.onOrderBook(out) }.onFailure { logger.error(it) { "kiwoom stream: 호가 리스너 오류 / $symbol" } } }
                }
                parseOrderEvents(node).forEach { event ->
                    usage?.streamMessage(StreamChannel.ORDER_EVENTS)
                    orderListeners.forEach { l -> runCatching { l.onOrderEvent(event) }.onFailure { logger.error(it) { "kiwoom stream: 주문 통보 리스너 오류 / ${event.orderId}" } } }
                }
            }
            else -> logger.debug { "kiwoom stream: ${text.take(200)}" }
        }
    }

    private fun registerMessage(items: List<String>, type: String): String =
        objectMapper.writeValueAsString(
            mapOf(
                "trnm" to "REG", "grp_no" to "1", "refresh" to "1",
                "data" to listOf(mapOf("item" to items, "type" to listOf(type))),
            ),
        )

    companion object {
        const val TYPE_TRADE = "0B"
        const val TYPE_ORDER_BOOK = "0D"
        const val TYPE_ORDER_EVENTS = "00"
        private val HHMMSS: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmmss")

        private fun JsonNode.signed(fid: String): BigDecimal? =
            path(fid).asText("").trim().removePrefix("+").takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

        private fun kstInstant(hhmmss: String, today: LocalDate): Instant =
            today.atTime(LocalTime.parse(hhmmss.trim().padStart(6, '0'), HHMMSS)).atZone(KrxCalendar.KST).toInstant()

        private fun code(item: JsonNode): String = item.path("item").asText().trim().removePrefix("A")

        private fun JsonNode.items(type: String): List<JsonNode> = path("data").filter { it.path("type").asText("") == type }

        /** `REAL` 프레임 → 체결 목록 (0B 만). 심볼은 종목코드 그대로 (요청 표기 복원은 스트림이 한다) */
        fun parseReal(node: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): List<TradeTick> =
            node.items(TYPE_TRADE).mapNotNull { item ->
                runCatching {
                    val v = item.path("values")
                    TradeTick(
                        symbol = code(item),
                        price = v.signed("10")?.abs() ?: error("no price"),
                        quantity = v.signed("15")?.abs() ?: BigDecimal.ZERO,
                        timestamp = kstInstant(v.path("20").asText(), today),
                        askPrice = v.signed("27")?.abs(),
                        bidPrice = v.signed("28")?.abs(),
                        cumulativeVolume = v.signed("13")?.abs()?.toLong(),
                        change = v.signed("11"),
                        changeRate = v.signed("12")?.divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN),
                    )
                }.getOrNull()
            }

        /** `REAL` 프레임 → 호가창 목록 (0D 만). 0 호가는 빈 단계로 보고 뺀다 */
        fun parseOrderBook(node: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): List<OrderBookTick> =
            node.items(TYPE_ORDER_BOOK).mapNotNull { item ->
                runCatching {
                    val v = item.path("values")
                    fun levels(priceFrom: Int, qtyFrom: Int): List<OrderBookLevel> =
                        (0 until 10).mapNotNull { i ->
                            val price = v.signed((priceFrom + i).toString())?.abs() ?: return@mapNotNull null
                            if (price.signum() == 0) return@mapNotNull null
                            OrderBookLevel(price, v.signed((qtyFrom + i).toString())?.abs() ?: BigDecimal.ZERO)
                        }
                    OrderBookTick(
                        symbol = code(item),
                        timestamp = kstInstant(v.path("21").asText(), today),
                        asks = levels(41, 61),
                        bids = levels(51, 71),
                        totalAskQuantity = v.signed("121")?.abs(),
                        totalBidQuantity = v.signed("125")?.abs(),
                    )
                }.getOrNull()
            }

        /** `REAL` 프레임 → 주문 통보 목록 (00 만) */
        fun parseOrderEvents(node: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): List<OrderEvent> =
            node.items(TYPE_ORDER_EVENTS).mapNotNull { item ->
                runCatching {
                    val v = item.path("values")
                    val status = v.path("913").asText("").trim()
                    val kind = v.path("905").asText("").trim()
                    val reason = v.path("919").asText("").trim().ifBlank { null }
                    val filledQty = v.signed("911")?.abs()
                    val type = when {
                        reason != null -> OrderEventType.REJECTED
                        status.contains("체결") && filledQty != null && filledQty.signum() > 0 -> OrderEventType.FILLED
                        kind.contains("취소") -> OrderEventType.CANCELED
                        kind.contains("정정") -> OrderEventType.MODIFIED
                        else -> OrderEventType.ACCEPTED
                    }
                    val original = v.path("904").asText("").trim().takeIf { it.isNotBlank() && it.trimStart('0').isNotEmpty() }
                    OrderEvent(
                        orderId = v.path("9203").asText("").trim(),
                        type = type,
                        timestamp = kstInstant(v.path("908").asText("0"), today),
                        symbol = v.path("9001").asText("").trim().removePrefix("A").ifBlank { null },
                        side = when (v.path("907").asText("").trim()) { "1" -> OrderSide.SELL; "2" -> OrderSide.BUY; else -> null },
                        quantity = if (type == OrderEventType.FILLED) filledQty else v.signed("900")?.abs(),
                        price = if (type == OrderEventType.FILLED) v.signed("910")?.abs() else v.signed("901")?.abs(),
                        remainingQuantity = v.signed("902")?.abs(),
                        originalOrderId = original,
                        reason = reason,
                    )
                }.getOrNull()
            }
    }
}
