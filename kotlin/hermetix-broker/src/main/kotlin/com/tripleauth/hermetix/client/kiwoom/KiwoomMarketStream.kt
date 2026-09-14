package com.tripleauth.hermetix.client.kiwoom

import com.fasterxml.jackson.databind.JsonNode
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
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * 키움 REST API 실시간 체결 스트림 (실시간 타입 `0B` 주식체결). 2026-09-14 모의투자 서버(mockapi) 장중 실측 통과.
 *
 * 프로토콜 (키움 REST API 가이드 — 실시간시세):
 * - 접속: 모의 `wss://mockapi.kiwoom.com:10000/api/dostk/websocket`, 실전 `wss://api.kiwoom.com:10000/…`
 * - 로그인: 접속 직후 `{"trnm":"LOGIN","token":<접근토큰>}` → `{"trnm":"LOGIN","return_code":0}`. REST 토큰을 그대로 쓴다
 * - 등록: `{"trnm":"REG","grp_no":"1","refresh":"1","data":[{"item":[코드…],"type":["0B"]}]}` → `{"trnm":"REG","return_code":0}`
 * - 데이터: `{"trnm":"REAL","data":[{"type":"0B","name":"주식체결","item":"005930","values":{"20":체결시각,"10":현재가,…}}]}`
 * - `{"trnm":"PING"}` 은 받은 그대로 되돌려 보낸다
 *
 * values 의 FID: 20 체결시각 HHMMSS, 10 현재가, 11 전일대비, 12 등락율(%), 27 최우선매도호가, 28 최우선매수호가,
 * 15 거래량(+매수/-매도 체결), 13 누적거래량. REST 와 같이 가격·호가에 등락 부호가 붙으므로 절대값으로 파싱한다.
 * 실측: LOGIN 응답에 `sor_yn` 이 추가로 오고, REAL 프레임은 `data` 키가 `trnm` 보다 앞에 온다(키 순서 무관). 실측 프레임은 `conformance/fixtures/kiwoom.json#stream`.
 */
class KiwoomMarketStream(
    private val properties: KiwoomApiProperties,
    private val objectMapper: ObjectMapper,
    private val token: () -> String,
) : ReconnectingWebSocket("kiwoom"), MarketStream {

    private val logger = KotlinLogging.logger { }

    private val listeners = ConcurrentHashMap<String, CopyOnWriteArrayList<TradeListener>>()
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
        val newCodes = mutableListOf<String>()
        symbols.forEach { symbol ->
            val code = MarketSymbol.code(symbol)
            requestedSymbols.putIfAbsent(code, symbol)
            listeners.computeIfAbsent(code) { newCodes += code; CopyOnWriteArrayList() } += listener
        }
        if (newCodes.isNotEmpty() && isConnected) send(registerMessage(newCodes))
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
                    if (listeners.isNotEmpty()) send(registerMessage(listeners.keys.toList()))
                } else {
                    logger.error { "kiwoom stream: 로그인 실패 return_code=$code ${node.path("return_msg").asText("")}" }
                }
            }
            "REG" -> {
                val code = node.path("return_code").asInt(-1)
                if (code == 0) logger.info { "kiwoom stream: registered ${listeners.keys}" }
                else logger.warn { "kiwoom stream: 등록 실패 return_code=$code ${node.path("return_msg").asText("")}" }
            }
            "REAL" -> parseReal(node).forEach { deliver(it) }
            else -> logger.debug { "kiwoom stream: ${text.take(200)}" }
        }
    }

    private fun deliver(tick: TradeTick) {
        val code = tick.symbol
        val symbol = requestedSymbols[code] ?: code
        val normalized = if (symbol == code) tick else tick.copy(symbol = symbol)
        listeners[code]?.forEach { listener ->
            runCatching { listener.onTrade(normalized) }
                .onFailure { logger.error(it) { "kiwoom stream: 리스너 오류 / $symbol" } }
        }
    }

    private fun registerMessage(codes: List<String>): String =
        objectMapper.writeValueAsString(
            mapOf(
                "trnm" to "REG", "grp_no" to "1", "refresh" to "1",
                "data" to listOf(mapOf("item" to codes, "type" to listOf(TYPE_TRADE))),
            ),
        )

    companion object {
        const val TYPE_TRADE = "0B"
        private val HHMMSS: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmmss")

        /** `REAL` 프레임 → 체결 목록 (0B 만). 심볼은 종목코드 그대로 (요청 표기 복원은 [deliver]) */
        fun parseReal(node: JsonNode, today: LocalDate = LocalDate.now(KrxCalendar.KST)): List<TradeTick> =
            node.path("data").mapNotNull { item ->
                if (item.path("type").asText("") != TYPE_TRADE) return@mapNotNull null
                runCatching { parseItem(item, today) }.getOrNull()
            }

        private fun parseItem(item: JsonNode, today: LocalDate): TradeTick {
            val v = item.path("values")
            fun signed(fid: String): BigDecimal? = v.path(fid).asText("").trim().removePrefix("+").takeIf { it.isNotBlank() }?.toBigDecimalOrNull()
            val time = LocalTime.parse(v.path("20").asText().trim().padStart(6, '0'), HHMMSS)
            return TradeTick(
                // 종목코드는 REST 와 같이 A 프리픽스가 붙어 올 수 있다 ("A005930")
                symbol = item.path("item").asText().trim().removePrefix("A"),
                price = signed("10")?.abs() ?: error("no price"),
                quantity = signed("15")?.abs() ?: BigDecimal.ZERO,
                timestamp = today.atTime(time).atZone(KrxCalendar.KST).toInstant(),
                askPrice = signed("27")?.abs(),
                bidPrice = signed("28")?.abs(),
                cumulativeVolume = signed("13")?.abs()?.toLong(),
                change = signed("11"),
                changeRate = signed("12")?.divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN),
            )
        }
    }
}
