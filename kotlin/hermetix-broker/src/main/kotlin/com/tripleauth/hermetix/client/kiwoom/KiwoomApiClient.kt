package com.tripleauth.hermetix.client.kiwoom

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.AuthError
import com.tripleauth.hermetix.broker.BrokerApiException
import com.tripleauth.hermetix.broker.BrokerCapabilities
import com.tripleauth.hermetix.broker.MarketClosedError
import com.tripleauth.hermetix.broker.OrderNotFoundError
import com.tripleauth.hermetix.broker.RateLimitError
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.KrxTick
import com.tripleauth.hermetix.broker.TradingEnvironment
import com.tripleauth.hermetix.broker.symbolCode
import com.tripleauth.hermetix.client.dto.AccountResponse
import com.tripleauth.hermetix.client.dto.BuyingPowerResponse
import com.tripleauth.hermetix.client.dto.CalendarResponse
import com.tripleauth.hermetix.client.dto.Candle
import com.tripleauth.hermetix.client.dto.CandleInterval
import com.tripleauth.hermetix.client.dto.CandlesResponse
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.Fill
import com.tripleauth.hermetix.client.dto.FillsResponse
import com.tripleauth.hermetix.client.dto.Holding
import com.tripleauth.hermetix.client.dto.HoldingsResponse
import com.tripleauth.hermetix.client.dto.OrderResponse
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderStatus
import com.tripleauth.hermetix.client.dto.OrderType
import com.tripleauth.hermetix.client.dto.OrdersResponse
import com.tripleauth.hermetix.client.dto.Quote
import com.tripleauth.hermetix.client.dto.QuotesResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.http.MediaType
import org.springframework.web.client.RestClient
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * 키움증권 REST 어댑터. `hermetix.broker: kiwoom` 으로 활성화한다.
 * 실전(`hermetix.kiwoom.environment: live`)은 호스트 api.kiwoom.com 을 쓰며 TR ID 는 모의와 같다.
 *
 * 실측 기반 구현 (mockapi.kiwoom.com, 2026-08):
 * - 모든 호출은 POST + `api-id` 헤더(TR)로 라우팅된다
 * - 응답 엔벨로프: return_code(0=성공) / return_msg
 * - 가격 필드에 등락 방향 부호가 붙는다 (cur_prc "-239500" = 하락 중인 239,500원) → 절대값 파싱
 * - 금액 필드는 zero-padded 문자열 ("000000100000000" = 1억)
 *
 * 제약:
 * - 모의투자는 KRX 국내주식만 지원한다
 * - 캔들은 일봉(DAY_1)만 지원한다
 * - 캘린더는 KRX 정규장을 합성한다 (공휴일 미반영 — 주문은 서버가 거부)
 * - clientOrderId 미지원 (무시된다)
 */
class KiwoomApiClient(
    private val properties: KiwoomApiProperties,
    private val objectMapper: ObjectMapper,
) : BrokerClient {

    private val logger = KotlinLogging.logger { }

    override val capabilities = BrokerCapabilities(
        brokerId = "kiwoom",
        market = "KRX",
        currency = "KRW",
        candleIntervals = setOf(CandleInterval.DAY_1),
        clientOrderId = false,
        nativeBracket = false,
        fractionalShares = false,
        environments = setOf(TradingEnvironment.PAPER, TradingEnvironment.LIVE),
    )

    override val environment: TradingEnvironment = properties.environment

    private val restClient = RestClient.builder()
        .baseUrl(properties.resolvedBaseUrl())
        .build()

    @Volatile
    private var cachedToken: Pair<String, Instant>? = null

    private val throttleLock = Object()
    private var lastCallAt: Long = 0

    // ------------------------------------------------------------------ market

    override fun getQuotes(symbols: List<String>): QuotesResponse {
        val quotes = symbols.map { symbol ->
            val node = call("/api/dostk/stkinfo", "ka10001", mapOf("stk_cd" to capabilities.symbolCode(symbol)))
            Quote(
                symbol = symbol,
                price = node.signedDecimal("cur_prc").abs(),
                bidPrice = null,
                askPrice = null,
                volume = node.signedDecimal("trde_qty").abs().toLong(),
                change = node.signedDecimalOrNull("pred_pre"),
                // flu_rt 는 % 단위 → 비율로 변환
                changeRate = node.signedDecimalOrNull("flu_rt")?.divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN),
                timestamp = Instant.now(),
            )
        }
        return QuotesResponse(quotes)
    }

    override fun getCandles(symbol: String, interval: CandleInterval, limit: Int?): CandlesResponse {
        require(interval == CandleInterval.DAY_1) {
            "키움 어댑터는 일봉(DAY_1)만 지원합니다."
        }

        val rows = call(
            "/api/dostk/chart", "ka10081",
            mapOf("stk_cd" to capabilities.symbolCode(symbol), "base_dt" to LocalDate.now(KrxCalendar.KST).format(DATE), "upd_stkpc_tp" to "1"),
        ).path("stk_dt_pole_chart_qry")

        // 키움은 최신순 → 공통 모델은 과거→최신
        val candles = rows.mapNotNull { row ->
            val date = row.path("dt").asText("")
            if (date.isBlank()) return@mapNotNull null
            Candle(
                timestamp = LocalDate.parse(date, DATE).atStartOfDay(KrxCalendar.KST).toInstant(),
                open = row.signedDecimal("open_pric").abs(),
                high = row.signedDecimal("high_pric").abs(),
                low = row.signedDecimal("low_pric").abs(),
                close = row.signedDecimal("cur_prc").abs(),
                volume = row.signedDecimal("trde_qty").abs().toLong(),
            )
        }.sortedBy { it.timestamp }.let { list -> limit?.let { list.takeLast(it) } ?: list }

        return CandlesResponse(symbol = symbol, interval = interval.value, candles = candles)
    }

    override fun getCalendar(): CalendarResponse = KrxCalendar.synthesize()

    // ----------------------------------------------------------------- account

    override fun getAccount(): AccountResponse {
        val deposit = call("/api/dostk/acnt", "kt00001", mapOf("qry_tp" to "3"))
        val balance = balanceSummary()

        val cash = deposit.paddedDecimal("entr")
        // 추정예탁자산(현금+평가) 이 있으면 사용, 없으면 현금+주식평가 합산
        val portfolio = balance.paddedDecimalOrNull("prsm_dpst_aset_amt")
            ?.takeIf { it > BigDecimal.ZERO }
            ?: (cash + (balance.paddedDecimalOrNull("tot_evlt_amt") ?: BigDecimal.ZERO))

        return AccountResponse(
            accountId = "kiwoom-mock",
            name = null,
            currency = "KRW",
            cash = cash,
            portfolioValue = portfolio,
            status = "ACTIVE",
        )
    }

    override fun getHoldings(): HoldingsResponse {
        val holdings = balanceSummary().path("acnt_evlt_remn_indv_tot")
            .mapNotNull { row ->
                val quantity = row.paddedDecimalOrNull("rmnd_qty") ?: return@mapNotNull null
                if (quantity <= BigDecimal.ZERO) return@mapNotNull null
                Holding(
                    symbol = row.path("stk_cd").asText().removePrefix("A"),
                    quantity = quantity,
                    avgEntryPrice = row.paddedDecimalOrNull("pur_pric") ?: BigDecimal.ZERO,
                    currentPrice = row.signedDecimalOrNull("cur_prc")?.abs(),
                    marketValue = row.paddedDecimalOrNull("evlt_amt"),
                    unrealizedPnl = row.signedDecimalOrNull("evltv_prft"),
                    unrealizedPnlRate = row.signedDecimalOrNull("prft_rt")?.divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN),
                )
            }
        return HoldingsResponse(holdings)
    }

    override fun getBuyingPower(): BuyingPowerResponse {
        val deposit = call("/api/dostk/acnt", "kt00001", mapOf("qry_tp" to "3"))
        return BuyingPowerResponse(
            accountId = "kiwoom-mock",
            currency = "KRW",
            buyingPower = deposit.paddedDecimalOrNull("ord_alow_amt") ?: deposit.paddedDecimal("entr"),
        )
    }

    // ------------------------------------------------------------------ orders

    override fun createOrder(request: CreateOrderRequest): OrderResponse {
        require(request.orderType == OrderType.LIMIT || request.orderType == OrderType.MARKET) {
            "키움 어댑터는 LIMIT/MARKET 주문만 지원합니다"
        }

        val apiId = if (request.side == OrderSide.BUY) "kt10000" else "kt10001"
        val code = capabilities.symbolCode(request.symbol)
        val node = call(
            "/api/dostk/ordr", apiId,
            mapOf(
                "dmst_stex_tp" to "KRX",
                "stk_cd" to code,
                "ord_qty" to request.quantity.toPlainString(),
                "ord_uv" to if (request.orderType == OrderType.LIMIT) KrxTick.round(request.limitPrice!!).toPlainString() else "",
                "trde_tp" to if (request.orderType == OrderType.LIMIT) "0" else "3",
                "cond_uv" to "",
            ),
        )

        return OrderResponse(
            orderId = node.path("ord_no").asText(),
            clientOrderId = request.clientOrderId,
            status = OrderStatus.SUBMITTED,
            symbol = code, // 보유/미체결과 같은 단일 시장 표기(접두 없음)
            side = request.side,
            orderType = request.orderType,
            quantity = request.quantity,
            limitPrice = request.limitPrice,
            filledQuantity = BigDecimal.ZERO,
            submittedAt = Instant.now(),
        )
    }

    override fun getOrders(): OrdersResponse =
        OrdersResponse(openOrders().map { it.toOrderResponse() })

    override fun getOrder(orderId: String): OrderResponse {
        openOrders().firstOrNull { it.orderNo() == orderId }?.let { return it.toOrderResponse() }

        // 미체결에 없으면 체결 내역에서 확인
        fills().firstOrNull { it.path("ord_no").asText().trimStart('0') == orderId.trimStart('0') }?.let { row ->
            return OrderResponse(
                orderId = orderId,
                status = OrderStatus.FILLED,
                symbol = row.path("stk_cd").asText().removePrefix("A"),
                filledQuantity = row.paddedDecimalOrNull("cntr_qty"),
                avgFillPrice = row.signedDecimalOrNull("cntr_pric")?.abs(),
            )
        }

        // 어느 쪽에도 없으면 취소된 것으로 간주
        return OrderResponse(orderId = orderId, status = OrderStatus.CANCELED)
    }

    override fun cancelOrder(orderId: String): OrderResponse {
        val order = openOrders().firstOrNull { it.orderNo() == orderId }
            ?: throw OrderNotFoundError("order-not-found", "키움 미체결 주문을 찾을 수 없습니다: $orderId")

        call(
            "/api/dostk/ordr", "kt10003",
            mapOf(
                "dmst_stex_tp" to "KRX",
                "orig_ord_no" to orderId,
                "stk_cd" to order.path("stk_cd").asText().removePrefix("A"),
                "cncl_qty" to "0", // 전량 취소
            ),
        )

        return OrderResponse(orderId = orderId, status = OrderStatus.CANCELED, canceledAt = Instant.now())
    }

    override fun getFills(): FillsResponse {
        val result = fills().map { row ->
            Fill(
                fillId = row.path("ord_no").asText(),
                orderId = row.path("ord_no").asText(),
                symbol = row.path("stk_cd").asText().removePrefix("A"),
                side = if (row.path("io_tp_nm").asText().contains("매수")) OrderSide.BUY else OrderSide.SELL,
                quantity = row.paddedDecimalOrNull("cntr_qty"),
                price = row.signedDecimalOrNull("cntr_pric")?.abs(),
                amount = null,
                timestamp = null,
            )
        }
        return FillsResponse(result)
    }

    // ---------------------------------------------------------------- internal

    private fun balanceSummary(): JsonNode =
        call("/api/dostk/acnt", "kt00018", mapOf("qry_tp" to "1", "dmst_stex_tp" to "KRX"))

    private fun openOrders(): List<JsonNode> =
        call("/api/dostk/acnt", "ka10075", mapOf("all_stk_tp" to "0", "trde_tp" to "0", "stk_cd" to "", "stex_tp" to "0"))
            .path("oso").toList()

    private fun fills(): List<JsonNode> =
        call("/api/dostk/acnt", "ka10076", mapOf("stk_cd" to "", "qry_tp" to "0", "sell_tp" to "0", "ord_no" to "", "stex_tp" to "0"))
            .path("cntr").toList()

    private fun JsonNode.orderNo(): String = path("ord_no").asText()

    private fun JsonNode.toOrderResponse(): OrderResponse {
        val ordQty = paddedDecimalOrNull("ord_qty") ?: BigDecimal.ZERO
        val remaining = paddedDecimalOrNull("oso_qty") ?: ordQty
        val filled = ordQty - remaining

        return OrderResponse(
            orderId = orderNo(),
            status = if (filled > BigDecimal.ZERO) OrderStatus.PARTIALLY_FILLED else OrderStatus.SUBMITTED,
            symbol = path("stk_cd").asText().removePrefix("A"),
            side = if (path("io_tp_nm").asText().contains("매수")) OrderSide.BUY else OrderSide.SELL,
            orderType = OrderType.LIMIT,
            quantity = ordQty,
            limitPrice = signedDecimalOrNull("ord_pric")?.abs(),
            filledQuantity = filled,
        )
    }

    private fun call(path: String, apiId: String, body: Map<String, String>): JsonNode {
        var attempt = 0
        while (true) {
            try {
                return callOnce(path, apiId, body)
            } catch (e: RateLimitError) {
                // 키움 유량 제한(초당 1회/TR) — 잠시 대기 후 재시도
                if (attempt < 3) {
                    attempt++
                    logger.warn { "키움 rate limit($apiId) - retry $attempt/3" }
                    Thread.sleep(1100L * attempt)
                } else {
                    throw e
                }
            }
        }
    }

    private fun callOnce(path: String, apiId: String, body: Map<String, String>): JsonNode {
        throttle()

        return restClient.post()
            .uri(path)
            .contentType(MediaType.APPLICATION_JSON)
            .headers { headers ->
                headers.set("authorization", "Bearer ${token()}")
                headers.set("api-id", apiId)
            }
            .body(objectMapper.writeValueAsString(body))
            .exchange { _, res ->
                val bytes = res.body.readAllBytes()
                val node = runCatching { objectMapper.readTree(bytes) }.getOrNull()
                    ?: throw BrokerApiException(res.statusCode.value(), null, "키움 응답 파싱 실패")
                if (!res.statusCode.is2xxSuccessful || node.path("return_code").asInt(-1) != 0) {
                    val code = node.path("return_code").asText(null)
                    val msg = "키움($apiId) ${node.path("return_msg").asText("")}".trim()
                    throw when {
                        msg.contains("요청 개수를 초과") -> RateLimitError(res.statusCode.value(), code, msg)
                        msg.contains("장종료") || msg.contains("RC4058") -> MarketClosedError(res.statusCode.value(), code, msg)
                        res.statusCode.value() == 401 -> AuthError(res.statusCode.value(), code, msg)
                        else -> BrokerApiException(res.statusCode.value(), code, msg)
                    }
                }
                node
            }!!
    }

    private fun throttle() {
        synchronized(throttleLock) {
            val wait = lastCallAt + properties.throttleMillis - System.currentTimeMillis()
            if (wait > 0) Thread.sleep(wait)
            lastCallAt = System.currentTimeMillis()
        }
    }

    private fun token(): String {
        val cached = cachedToken
        if (cached != null && cached.second.isAfter(Instant.now().plusSeconds(properties.tokenRefreshMarginSeconds))) {
            return cached.first
        }
        return refreshToken()
    }

    @Synchronized
    private fun refreshToken(): String {
        val cached = cachedToken
        if (cached != null && cached.second.isAfter(Instant.now().plusSeconds(properties.tokenRefreshMarginSeconds))) {
            return cached.first
        }

        throttle()
        val node = restClient.post()
            .uri("/oauth2/token")
            .contentType(MediaType.APPLICATION_JSON)
            .body(
                objectMapper.writeValueAsString(
                    mapOf("grant_type" to "client_credentials", "appkey" to properties.appkey, "secretkey" to properties.secretkey),
                ),
            )
            .exchange { _, res ->
                val n = objectMapper.readTree(res.body.readAllBytes())
                if (!res.statusCode.is2xxSuccessful || n.path("return_code").asInt(-1) != 0 || !n.hasNonNull("token")) {
                    throw AuthError(res.statusCode.value(), n.path("return_code").asText(null), "키움 토큰 발급 실패: ${n.path("return_msg").asText("")}")
                }
                n
            }!!

        val token = node.path("token").asText()
        // expires_dt: yyyyMMddHHmmss (KST)
        val expiresAt = runCatching {
            LocalDateTime.parse(node.path("expires_dt").asText(), DATETIME).atZone(KrxCalendar.KST).toInstant()
        }.getOrDefault(Instant.now().plusSeconds(86400))

        cachedToken = token to expiresAt
        logger.info { "키움 token refreshed / expiresAt=$expiresAt" }
        return token
    }

    /** 등락 부호가 붙는 필드 ("-239500", "+1200") — 부호 유지 파싱 */
    private fun JsonNode.signedDecimal(field: String): BigDecimal =
        path(field).asText("0").ifBlank { "0" }.removePrefix("+").toBigDecimal()

    private fun JsonNode.signedDecimalOrNull(field: String): BigDecimal? =
        path(field).asText("").takeIf { it.isNotBlank() }?.removePrefix("+")?.toBigDecimalOrNull()

    /** zero-padded 금액 ("000000100000000") 파싱 */
    private fun JsonNode.paddedDecimal(field: String): BigDecimal =
        path(field).asText("0").ifBlank { "0" }.toBigDecimal()

    private fun JsonNode.paddedDecimalOrNull(field: String): BigDecimal? =
        path(field).asText("").takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

    companion object {
        private val DATE: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        private val DATETIME: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    }
}
