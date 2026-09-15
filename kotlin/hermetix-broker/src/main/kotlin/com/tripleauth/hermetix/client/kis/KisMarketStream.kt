package com.tripleauth.hermetix.client.kis

import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.BrokerUsage
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.StreamChannel
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
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * KIS 실시간 스트림. 체결가 `H0STCNT0`·호가 `H0STASP0` 는 2026-09-14 모의투자 서버(ops…:31000) 장중 실측 통과,
 * 주문 통보 `H0STCNI9`(모의)/`H0STCNI0`(실전)는 문서 기반 (HTS ID 로 구독해야 실측 가능).
 *
 * 프로토콜 (KIS 개발자센터 웹소켓 가이드):
 * - 접속: 모의 `ws://ops.koreainvestment.com:31000`, 실전 `:21000`. TLS 없음
 * - 인증: REST `POST /oauth2/Approval` 로 받은 `approval_key` 를 구독 메시지 헤더에 싣는다 (매 접속마다 재발급 가능)
 * - 구독: `{"header":{"approval_key","custtype","tr_type":"1"|"2","content-type":"utf-8"},"body":{"input":{"tr_id","tr_key"}}}`
 *   tr_key 는 시세 TR 이면 종목코드, 주문 통보 TR 이면 **HTS ID**
 * - 데이터 프레임: `0|TR|<건수>|<필드^필드^…>` — 첫 세그먼트 0 은 평문, 1 은 AES 암호문(base64).
 *   레코드가 여러 건이면 본문에 이어 붙는다 (필드 폭 = 전체 필드 수 / 건수). 실측: 체결가는 한 프레임에 최대 3건
 * - 제어 프레임(JSON): 구독 결과(`body.rt_cd`/`msg_cd`, 암호화 TR 이면 `output.iv/key`), `PINGPONG`(그대로 되돌려 보내야 연결 유지)
 *
 * 필드 순서:
 * - H0STCNT0 (폭 47): 0 코드, 1 시각 HHMMSS, 2 현재가, 3 전일대비부호, 4 전일대비(부호 포함), 5 전일대비율(%), … 10 매도호가1, 11 매수호가1, 12 체결량, 13 누적거래량
 * - H0STASP0 (실측 폭 63): 0 코드, 1 시각, 2 시간구분, 3–12 매도호가1–10, 13–22 매수호가1–10, 23–32 매도잔량1–10, 33–42 매수잔량1–10, 43 총매도잔량, 44 총매수잔량
 * - H0STCNI9/0 (AES-256-CBC, 구독 응답의 key/iv): 0 고객ID, 1 계좌번호, 2 주문번호, 3 원주문번호, 4 매도매수구분(01 매도/02 매수), 5 정정취소구분(0/1 정정/2 취소),
 *   6 주문종류, 7 주문조건, 8 종목코드, 9 체결수량, 10 체결단가, 11 체결시각, 12 거부여부(0/1), 13 체결여부(1 접수·정정·취소·거부 / 2 체결), 14 접수여부, 15 지점, 16 주문수량, … 25 주문가격
 *
 * 실측 프레임은 `conformance/fixtures/kis.json#stream`.
 */
class KisMarketStream(
    private val properties: KisApiProperties,
    private val objectMapper: ObjectMapper,
    private val approvalKey: () -> String,
    usage: BrokerUsage? = null,
) : ReconnectingWebSocket("kis", usage = usage), MarketStream {

    private val logger = KotlinLogging.logger { }

    private val tradeListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<TradeListener>>()
    private val bookListeners = ConcurrentHashMap<String, CopyOnWriteArrayList<OrderBookListener>>()
    private val orderListeners = CopyOnWriteArrayList<OrderEventListener>()

    /** 종목코드 → 구독 요청 표기 */
    private val requestedSymbols = ConcurrentHashMap<String, String>()

    /** 암호화 TR 의 복호화 키 (구독 응답 output.key/iv) — TR 별 */
    private val cipherKeys = ConcurrentHashMap<String, Pair<String, String>>()

    private val trOrderEvents: String get() = if (properties.isLive) TR_ORDER_EVENTS_LIVE else TR_ORDER_EVENTS_PAPER

    override val isConnected: Boolean get() = isSocketOpen

    override fun uri(): URI = URI.create(properties.resolvedWsUrl())

    override fun onOpen() {
        val key = approvalKey()
        tradeListeners.keys.forEach { code -> send(subscribeMessage(key, TR_TRADE, code)) }
        bookListeners.keys.forEach { code -> send(subscribeMessage(key, TR_ORDER_BOOK, code)) }
        if (orderListeners.isNotEmpty()) send(subscribeMessage(key, trOrderEvents, properties.htsId))
    }

    override fun subscribeTrades(symbols: List<String>, listener: TradeListener) {
        val newCodes = register(symbols, listener, tradeListeners)
        usage?.streamSubscribed(StreamChannel.TRADES, newCodes.size)
        if (newCodes.isNotEmpty() && isSocketOpen) {
            val key = approvalKey()
            newCodes.forEach { send(subscribeMessage(key, TR_TRADE, it)) }
        }
    }

    override fun subscribeOrderBook(symbols: List<String>, listener: OrderBookListener) {
        val newCodes = register(symbols, listener, bookListeners)
        usage?.streamSubscribed(StreamChannel.ORDER_BOOK, newCodes.size)
        if (newCodes.isNotEmpty() && isSocketOpen) {
            val key = approvalKey()
            newCodes.forEach { send(subscribeMessage(key, TR_ORDER_BOOK, it)) }
        }
    }

    override fun subscribeOrderEvents(listener: OrderEventListener) {
        check(properties.htsId.isNotBlank()) { "KIS 주문 통보 구독에는 HTS ID 가 필요합니다 (hermetix.kis.hts-id)" }
        val first = orderListeners.isEmpty()
        orderListeners += listener
        if (first) usage?.streamSubscribed(StreamChannel.ORDER_EVENTS)
        if (first && isSocketOpen) send(subscribeMessage(approvalKey(), trOrderEvents, properties.htsId))
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
        if (text.startsWith("0|") || text.startsWith("1|")) {
            onDataFrame(text)
            return
        }
        val node = runCatching { objectMapper.readTree(text) }.getOrNull() ?: return
        val header = node.path("header")
        val trId = header.path("tr_id").asText("")
        if (trId == "PINGPONG") {
            send(text)
            return
        }
        val body = node.path("body")
        if (body.isMissingNode) return
        val rtCd = body.path("rt_cd").asText("")
        val msgCd = body.path("msg_cd").asText("")
        val msg = body.path("msg1").asText("")
        val trKey = header.path("tr_key").asText("")
        val output = body.path("output")
        if (output.hasNonNull("key") && output.hasNonNull("iv")) {
            cipherKeys[trId] = output.path("key").asText() to output.path("iv").asText()
        }
        when {
            rtCd == "0" || msgCd == "OPSP0000" -> logger.info { "kis stream: subscribed $trId ${if (trId.startsWith("H0STCNI")) "(hts)" else trKey} ($msg)" }
            msgCd == "OPSP0002" -> logger.info { "kis stream: already subscribed $trId" }
            else -> logger.warn { "kis stream: $trId $trKey rt_cd=$rtCd msg_cd=$msgCd $msg" }
        }
    }

    private fun onDataFrame(text: String) {
        val parts = text.split("|", limit = 4)
        if (parts.size < 4) return
        val trId = parts[1]
        val body = if (parts[0] == "1") {
            val (key, iv) = cipherKeys[trId] ?: run {
                logger.warn { "kis stream: 암호화 프레임($trId)인데 복호화 키가 없다 — 구독 응답 전 프레임?" }
                return
            }
            runCatching { decrypt(parts[3], key, iv) }
                .onFailure { logger.warn { "kis stream: 복호화 실패($trId) - ${it.message}" } }
                .getOrNull() ?: return
        } else {
            parts[3]
        }
        val plain = "0|$trId|${parts[2]}|$body"
        when (trId) {
            TR_TRADE -> parseFrame(plain).forEach { tick ->
                usage?.streamMessage(StreamChannel.TRADES)
                val symbol = requestedSymbols[tick.symbol] ?: tick.symbol
                val out = if (symbol == tick.symbol) tick else tick.copy(symbol = symbol)
                tradeListeners[tick.symbol]?.forEach { l -> runCatching { l.onTrade(out) }.onFailure { logger.error(it) { "kis stream: 리스너 오류 / $symbol" } } }
            }
            TR_ORDER_BOOK -> parseOrderBookFrame(plain).forEach { tick ->
                usage?.streamMessage(StreamChannel.ORDER_BOOK)
                val symbol = requestedSymbols[tick.symbol] ?: tick.symbol
                val out = if (symbol == tick.symbol) tick else tick.copy(symbol = symbol)
                bookListeners[tick.symbol]?.forEach { l -> runCatching { l.onOrderBook(out) }.onFailure { logger.error(it) { "kis stream: 호가 리스너 오류 / $symbol" } } }
            }
            TR_ORDER_EVENTS_PAPER, TR_ORDER_EVENTS_LIVE -> parseOrderEventFrame(plain).forEach { event ->
                usage?.streamMessage(StreamChannel.ORDER_EVENTS)
                orderListeners.forEach { l -> runCatching { l.onOrderEvent(event) }.onFailure { logger.error(it) { "kis stream: 주문 통보 리스너 오류 / ${event.orderId}" } } }
            }
            else -> logger.debug { "kis stream: unknown tr $trId" }
        }
    }

    private fun subscribeMessage(key: String, trId: String, trKey: String, trType: String = "1"): String =
        objectMapper.writeValueAsString(
            mapOf(
                "header" to mapOf("approval_key" to key, "custtype" to properties.custtype, "tr_type" to trType, "content-type" to "utf-8"),
                "body" to mapOf("input" to mapOf("tr_id" to trId, "tr_key" to trKey)),
            ),
        )

    companion object {
        const val TR_TRADE = "H0STCNT0"
        const val TR_ORDER_BOOK = "H0STASP0"
        const val TR_ORDER_EVENTS_PAPER = "H0STCNI9"
        const val TR_ORDER_EVENTS_LIVE = "H0STCNI0"
        private const val MIN_TRADE_FIELDS = 14
        private const val MIN_BOOK_FIELDS = 45
        private const val MIN_ORDER_FIELDS = 17
        private val HHMMSS: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmmss")

        /** `0|TR|건수|본문` 을 레코드(필드 목록)로 나눈다. TR 불일치·필드 부족이면 빈 목록 */
        private fun records(frame: String, trId: String, minFields: Int): List<List<String>> {
            val parts = frame.split("|", limit = 4)
            if (parts.size < 4 || parts[1] != trId) return emptyList()
            val count = parts[2].toIntOrNull()?.takeIf { it > 0 } ?: 1
            val fields = parts[3].split("^")
            val width = fields.size / count
            if (width < minFields) return emptyList()
            return (0 until count).map { i -> fields.subList(i * width, (i + 1) * width) }
        }

        private fun kstInstant(hhmmss: String, today: LocalDate): Instant =
            today.atTime(LocalTime.parse(hhmmss.padStart(6, '0'), HHMMSS)).atZone(KrxCalendar.KST).toInstant()

        /** 체결가 프레임 → 체결 목록. 심볼은 단축코드 그대로 (요청 표기 복원은 스트림이 한다) */
        fun parseFrame(frame: String, today: LocalDate = LocalDate.now(KrxCalendar.KST)): List<TradeTick> =
            records(frame, TR_TRADE, MIN_TRADE_FIELDS).mapNotNull { f -> runCatching { parseTrade(f, today) }.getOrNull() }

        private fun parseTrade(f: List<String>, today: LocalDate): TradeTick {
            val sign = f[3]
            val rawChange = f[4].toBigDecimalOrNull()
            // 부호 코드: 1 상한 2 상승 3 보합 4 하한 5 하락. 값이 부호 없이 오면 코드로 부호를 붙인다 (실측은 부호 포함)
            val change = rawChange?.let { if ((sign == "4" || sign == "5") && it.signum() > 0) it.negate() else it }
            return TradeTick(
                symbol = f[0],
                price = f[2].toBigDecimal(),
                quantity = f[12].toBigDecimal(),
                timestamp = kstInstant(f[1], today),
                askPrice = f[10].toBigDecimalOrNull(),
                bidPrice = f[11].toBigDecimalOrNull(),
                cumulativeVolume = f[13].toLongOrNull(),
                change = change,
                changeRate = f[5].toBigDecimalOrNull()?.divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN),
            )
        }

        /** 호가 프레임(H0STASP0) → 호가창 목록. 0 호가는 빈 단계로 보고 뺀다 */
        fun parseOrderBookFrame(frame: String, today: LocalDate = LocalDate.now(KrxCalendar.KST)): List<OrderBookTick> =
            records(frame, TR_ORDER_BOOK, MIN_BOOK_FIELDS).mapNotNull { f ->
                runCatching {
                    fun levels(priceFrom: Int, qtyFrom: Int): List<OrderBookLevel> =
                        (0 until 10).mapNotNull { i ->
                            val price = f[priceFrom + i].toBigDecimalOrNull() ?: return@mapNotNull null
                            if (price.signum() == 0) return@mapNotNull null
                            OrderBookLevel(price, f[qtyFrom + i].toBigDecimalOrNull() ?: BigDecimal.ZERO)
                        }
                    OrderBookTick(
                        symbol = f[0],
                        timestamp = kstInstant(f[1], today),
                        asks = levels(3, 23),
                        bids = levels(13, 33),
                        totalAskQuantity = f[43].toBigDecimalOrNull(),
                        totalBidQuantity = f[44].toBigDecimalOrNull(),
                    )
                }.getOrNull()
            }

        /** 주문 통보 프레임(복호화된 H0STCNI9/H0STCNI0) → 이벤트 목록 */
        fun parseOrderEventFrame(frame: String, today: LocalDate = LocalDate.now(KrxCalendar.KST)): List<OrderEvent> {
            val trId = frame.split("|", limit = 3).getOrNull(1) ?: return emptyList()
            if (trId != TR_ORDER_EVENTS_PAPER && trId != TR_ORDER_EVENTS_LIVE) return emptyList()
            return records(frame, trId, MIN_ORDER_FIELDS).mapNotNull { f -> runCatching { parseOrderEvent(f, today) }.getOrNull() }
        }

        private fun parseOrderEvent(f: List<String>, today: LocalDate): OrderEvent {
            val rejected = f[12] == "1"
            val filled = f[13] == "2"
            val amendKind = f[5] // 0 정상, 1 정정, 2 취소
            val type = when {
                rejected -> OrderEventType.REJECTED
                filled -> OrderEventType.FILLED
                amendKind == "2" -> OrderEventType.CANCELED
                amendKind == "1" -> OrderEventType.MODIFIED
                else -> OrderEventType.ACCEPTED
            }
            val quantity = if (filled) f[9].toBigDecimalOrNull() else f[16].toBigDecimalOrNull()
            val price = if (filled) f[10].toBigDecimalOrNull() else f.getOrNull(25)?.toBigDecimalOrNull()
            val original = f[3].trim().takeIf { it.isNotBlank() && it.trimStart('0').isNotEmpty() && it != f[2] }
            return OrderEvent(
                orderId = f[2].trim(),
                type = type,
                timestamp = kstInstant(f[11].trim(), today),
                symbol = f[8].trim(),
                side = when (f[4]) { "01" -> OrderSide.SELL; "02" -> OrderSide.BUY; else -> null },
                quantity = quantity,
                price = price,
                originalOrderId = original,
            )
        }

        /** KIS 암호화 본문: AES-256-CBC, PKCS5, base64. key 32자·iv 16자는 구독 응답 output 에서 온다 */
        fun decrypt(base64: String, key: String, iv: String): String {
            val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
            cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(key.toByteArray(Charsets.UTF_8), "AES"), IvParameterSpec(iv.toByteArray(Charsets.UTF_8)))
            return String(cipher.doFinal(Base64.getDecoder().decode(base64)), Charsets.UTF_8)
        }

        /** 테스트·픽스처 생성용 — [decrypt] 의 역 */
        fun encrypt(plain: String, key: String, iv: String): String {
            val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
            cipher.init(Cipher.ENCRYPT_MODE, SecretKeySpec(key.toByteArray(Charsets.UTF_8), "AES"), IvParameterSpec(iv.toByteArray(Charsets.UTF_8)))
            return Base64.getEncoder().encodeToString(cipher.doFinal(plain.toByteArray(Charsets.UTF_8)))
        }
    }
}
