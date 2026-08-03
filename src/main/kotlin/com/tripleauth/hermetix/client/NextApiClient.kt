package com.tripleauth.hermetix.client

import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.client.dto.AccountResponse
import com.tripleauth.hermetix.client.dto.ApiErrorEnvelope
import com.tripleauth.hermetix.client.dto.BuyingPowerResponse
import com.tripleauth.hermetix.client.dto.CalendarResponse
import com.tripleauth.hermetix.client.dto.CandleInterval
import com.tripleauth.hermetix.client.dto.CandlesResponse
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.ExchangeRateResponse
import com.tripleauth.hermetix.client.dto.FillsResponse
import com.tripleauth.hermetix.client.dto.HoldingsResponse
import com.tripleauth.hermetix.client.dto.InstrumentDetailResponse
import com.tripleauth.hermetix.client.dto.InstrumentsResponse
import com.tripleauth.hermetix.client.dto.OrderResponse
import com.tripleauth.hermetix.client.dto.OrdersResponse
import com.tripleauth.hermetix.client.dto.PreviewOrderRequest
import com.tripleauth.hermetix.client.dto.PreviewOrderResponse
import com.tripleauth.hermetix.client.dto.QuotesResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.web.client.RestClient
import org.springframework.web.util.UriBuilder
import java.net.URI

/**
 * 넥스트증권 OpenAPI v1 클라이언트.
 *
 * 모든 호출은 Bearer 토큰을 자동 첨부하며, 401 발생 시 토큰을 1회 재발급 후 재시도한다.
 * 계좌 관련 API 는 X-Nextsecurities-Account 헤더를 자동으로 붙인다.
 */
class NextApiClient(
    private val properties: NextApiProperties,
    private val tokenManager: TokenManager,
    private val objectMapper: ObjectMapper,
) : BrokerClient {

    private val logger = KotlinLogging.logger { }

    private val restClient = RestClient.builder()
        .baseUrl(properties.baseUrl)
        .build()

    // ------------------------------------------------------------------ market

    override fun getQuotes(symbols: List<String>): QuotesResponse =
        get(auth = true) { it.path("/v1/market/quotes").queryParam("symbols", symbols.joinToString(",")).build() }

    override fun getCandles(symbol: String, interval: CandleInterval, limit: Int?): CandlesResponse =
        get(auth = true) {
            it.path("/v1/market/candles")
                .queryParam("symbol", symbol)
                .queryParam("interval", interval.value)
                .apply { if (limit != null) queryParam("limit", limit) }
                .build()
        }

    override fun getCalendar(): CalendarResponse =
        get(auth = true) { it.path("/v1/market/calendar").build() }

    fun getExchangeRate(base: String = "USD", quote: String = "KRW"): ExchangeRateResponse =
        get(auth = true) { it.path("/v1/market/exchange-rate").queryParam("base", base).queryParam("quote", quote).build() }

    fun getInstruments(search: String? = null, cursor: String? = null, limit: Int? = null): InstrumentsResponse =
        get(auth = true) {
            it.path("/v1/instruments")
                .apply { if (search != null) queryParam("search", search) }
                .apply { if (cursor != null) queryParam("cursor", cursor) }
                .apply { if (limit != null) queryParam("limit", limit) }
                .build()
        }

    fun getInstrument(symbol: String): InstrumentDetailResponse =
        get(auth = true) { it.path("/v1/instruments/{symbol}").build(symbol) }

    // ----------------------------------------------------------------- account

    override fun getAccount(): AccountResponse =
        get(auth = true, account = true) { it.path("/v1/account").build() }

    override fun getHoldings(): HoldingsResponse =
        get(auth = true, account = true) { it.path("/v1/account/holdings").build() }

    override fun getBuyingPower(): BuyingPowerResponse =
        get(auth = true, account = true) { it.path("/v1/account/buying-power").build() }

    // ------------------------------------------------------------------ orders

    fun previewOrder(request: PreviewOrderRequest): PreviewOrderResponse =
        exchange(HttpMethod.POST, { it.path("/v1/orders/preview").build() }, request, account = true)

    override fun createOrder(request: CreateOrderRequest): OrderResponse =
        exchange(HttpMethod.POST, { it.path("/v1/orders").build() }, request, account = true)

    override fun getOrders(): OrdersResponse =
        get(auth = true, account = true) { it.path("/v1/orders").build() }

    override fun getOrder(orderId: String): OrderResponse =
        get(auth = true, account = true) { it.path("/v1/orders/{orderId}").build(orderId) }

    override fun cancelOrder(orderId: String): OrderResponse =
        exchange(HttpMethod.DELETE, { it.path("/v1/orders/{orderId}").build(orderId) }, null, account = true)

    override fun getFills(): FillsResponse =
        get(auth = true, account = true) { it.path("/v1/orders/fills").build() }

    // ---------------------------------------------------------------- internal

    private inline fun <reified T : Any> get(
        auth: Boolean = true,
        account: Boolean = false,
        noinline uri: (UriBuilder) -> URI,
    ): T = exchange(HttpMethod.GET, uri, null, auth, account)

    private inline fun <reified T : Any> exchange(
        method: HttpMethod,
        noinline uri: (UriBuilder) -> URI,
        body: Any?,
        auth: Boolean = true,
        account: Boolean = false,
    ): T = executeWithRetry(auth) { token ->
        restClient.method(method)
            .uri(uri)
            .apply {
                if (token != null) header("Authorization", "Bearer $token")
                if (account && properties.accountId.isNotBlank()) header("X-Nextsecurities-Account", properties.accountId)
                if (body != null) {
                    contentType(MediaType.APPLICATION_JSON)
                    body(body)
                }
            }
            .exchange { _, res ->
                val bytes = res.body.readAllBytes()
                if (res.statusCode.is2xxSuccessful) {
                    objectMapper.readValue(bytes, T::class.java)
                } else {
                    val error = runCatching { objectMapper.readValue(bytes, ApiErrorEnvelope::class.java).error }.getOrNull()
                    throw NextApiException(res.statusCode.value(), error)
                }
            }!!
    }

    private fun <T> executeWithRetry(auth: Boolean, call: (String?) -> T): T {
        val token = if (auth) tokenManager.getToken() else null
        return try {
            call(token)
        } catch (e: NextApiException) {
            if (auth && e.isAuthError) {
                logger.warn { "auth error(${e.code}) - refreshing token and retrying once" }
                tokenManager.invalidate()
                call(tokenManager.getToken())
            } else {
                throw e
            }
        }
    }
}
