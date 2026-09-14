package com.tripleauth.hermetix.client.kis

import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.MarketStream
import com.tripleauth.hermetix.broker.MarketSymbol
import com.tripleauth.hermetix.broker.ReconnectingWebSocket
import com.tripleauth.hermetix.broker.TradeListener
import com.tripleauth.hermetix.broker.TradeTick
import io.github.oshai.kotlinlogging.KotlinLogging
import java.math.BigDecimal
import java.math.RoundingMode
import java.net.URI
import java.time.LocalDate
import java.time.LocalTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * KIS 실시간 체결가 스트림 (TR `H0STCNT0`). 2026-09-14 모의투자 서버(ops…:31000) 장중 실측 통과.
 *
 * 프로토콜 (KIS 개발자센터 웹소켓 가이드):
 * - 접속: 모의 `ws://ops.koreainvestment.com:31000`, 실전 `:21000`. TLS 없음
 * - 인증: REST `POST /oauth2/Approval` 로 받은 `approval_key` 를 구독 메시지 헤더에 싣는다 (매 접속마다 재발급 가능)
 * - 구독: `{"header":{"approval_key","custtype","tr_type":"1"|"2","content-type":"utf-8"},"body":{"input":{"tr_id":"H0STCNT0","tr_key":"005930"}}}`
 * - 데이터 프레임: `0|H0STCNT0|<건수>|<필드^필드^…>` — 암호화 플래그(0/1)·TR·레코드 수·`^` 구분 본문.
 *   레코드가 여러 건이면 본문에 이어 붙는다 (필드 폭 = 전체 필드 수 / 건수)
 * - 제어 프레임(JSON): 구독 결과(`body.rt_cd`/`msg_cd`), `PINGPONG`(그대로 되돌려 보내야 연결 유지)
 *
 * 필드 순서(H0STCNT0, 0부터, 실측 레코드 폭 47): 0 단축코드, 1 체결시각 HHMMSS, 2 현재가, 3 전일대비부호, 4 전일대비(부호 포함),
 * 5 전일대비율(%), 6 가중평균가, 7 시가, 8 고가, 9 저가, 10 매도호가1, 11 매수호가1, 12 체결거래량, 13 누적거래량, …
 * 실측: 한 프레임에 레코드가 최대 3건 이어 붙어 온다(건수 세그먼트 003). 구독 성공 응답의 `output.iv/key` 는 암호화 TR 용이라 쓰지 않는다.
 * 실측 프레임은 `conformance/fixtures/kis.json#stream`.
 */
class KisMarketStream(
    private val properties: KisApiProperties,
    private val objectMapper: ObjectMapper,
    private val approvalKey: () -> String,
) : ReconnectingWebSocket("kis"), MarketStream {

    private val logger = KotlinLogging.logger { }

    /** 종목코드 → 리스너 (구독 요청 표기는 [requestedSymbols]) */
    private val listeners = ConcurrentHashMap<String, CopyOnWriteArrayList<TradeListener>>()
    private val requestedSymbols = ConcurrentHashMap<String, String>()

    override val isConnected: Boolean get() = isSocketOpen

    override fun uri(): URI = URI.create(properties.resolvedWsUrl())

    override fun onOpen() {
        val key = approvalKey()
        listeners.keys.forEach { code -> send(subscribeMessage(key, code)) }
    }

    override fun subscribeTrades(symbols: List<String>, listener: TradeListener) {
        val newCodes = mutableListOf<String>()
        symbols.forEach { symbol ->
            val code = MarketSymbol.code(symbol)
            requestedSymbols.putIfAbsent(code, symbol)
            val list = listeners.computeIfAbsent(code) { newCodes += code; CopyOnWriteArrayList() }
            list += listener
        }
        if (newCodes.isNotEmpty() && isSocketOpen) {
            val key = approvalKey()
            newCodes.forEach { send(subscribeMessage(key, it)) }
        }
    }

    override fun onMessage(text: String) {
        if (text.startsWith("0|") || text.startsWith("1|")) {
            parseFrame(text).forEach { tick -> deliver(tick) }
            return
        }
        val node = runCatching { objectMapper.readTree(text) }.getOrNull() ?: return
        val trId = node.path("header").path("tr_id").asText("")
        if (trId == "PINGPONG") {
            send(text)
            return
        }
        val body = node.path("body")
        if (body.isMissingNode) return
        val rtCd = body.path("rt_cd").asText("")
        val msgCd = body.path("msg_cd").asText("")
        val msg = body.path("msg1").asText("")
        val trKey = node.path("header").path("tr_key").asText("")
        when {
            rtCd == "0" || msgCd == "OPSP0000" -> logger.info { "kis stream: subscribed $trId $trKey ($msg)" }
            msgCd == "OPSP0002" -> logger.info { "kis stream: already subscribed $trId $trKey" }
            else -> logger.warn { "kis stream: $trId $trKey rt_cd=$rtCd msg_cd=$msgCd $msg" }
        }
    }

    private fun deliver(tick: TradeTick) {
        val code = tick.symbol
        val symbol = requestedSymbols[code] ?: code
        val normalized = if (symbol == code) tick else tick.copy(symbol = symbol)
        listeners[code]?.forEach { listener ->
            runCatching { listener.onTrade(normalized) }
                .onFailure { logger.error(it) { "kis stream: 리스너 오류 / $symbol" } }
        }
    }

    private fun subscribeMessage(key: String, code: String, trType: String = "1"): String =
        objectMapper.writeValueAsString(
            mapOf(
                "header" to mapOf("approval_key" to key, "custtype" to properties.custtype, "tr_type" to trType, "content-type" to "utf-8"),
                "body" to mapOf("input" to mapOf("tr_id" to TR_TRADE, "tr_key" to code)),
            ),
        )

    companion object {
        const val TR_TRADE = "H0STCNT0"
        private const val MIN_FIELDS = 14

        /**
         * 데이터 프레임 → 체결 목록. 심볼은 단축코드 그대로 (요청 표기 복원은 [deliver] 가 한다).
         * 알 수 없는 TR·필드 부족은 빈 목록.
         */
        fun parseFrame(frame: String, today: LocalDate = LocalDate.now(KrxCalendar.KST)): List<TradeTick> {
            val parts = frame.split("|", limit = 4)
            if (parts.size < 4 || parts[1] != TR_TRADE) return emptyList()
            val count = parts[2].toIntOrNull()?.takeIf { it > 0 } ?: 1
            val fields = parts[3].split("^")
            val width = fields.size / count
            if (width < MIN_FIELDS) return emptyList()
            return (0 until count).mapNotNull { i ->
                val f = fields.subList(i * width, (i + 1) * width)
                runCatching { parseRecord(f, today) }.getOrNull()
            }
        }

        private fun parseRecord(f: List<String>, today: LocalDate): TradeTick {
            val time = LocalTime.parse(f[1].padStart(6, '0'), java.time.format.DateTimeFormatter.ofPattern("HHmmss"))
            val sign = f[3]
            val rawChange = f[4].toBigDecimalOrNull()
            // 부호 코드: 1 상한 2 상승 3 보합 4 하한 5 하락. 값이 부호 없이 오면 코드로 부호를 붙인다
            val change = rawChange?.let { if ((sign == "4" || sign == "5") && it.signum() > 0) it.negate() else it }
            return TradeTick(
                symbol = f[0],
                price = f[2].toBigDecimal(),
                quantity = f[12].toBigDecimal(),
                timestamp = today.atTime(time).atZone(KrxCalendar.KST).toInstant(),
                askPrice = f[10].toBigDecimalOrNull(),
                bidPrice = f[11].toBigDecimalOrNull(),
                cumulativeVolume = f[13].toLongOrNull(),
                change = change,
                changeRate = f[5].toBigDecimalOrNull()?.divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN),
            )
        }
    }
}
