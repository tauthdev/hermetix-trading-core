package com.tripleauth.nexttrading

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.nexttrading.client.NextApiClient
import com.tripleauth.nexttrading.client.NextApiProperties
import com.tripleauth.nexttrading.client.TokenManager
import com.tripleauth.nexttrading.client.dto.CandleInterval
import com.tripleauth.nexttrading.client.dto.CreateOrderRequest
import com.tripleauth.nexttrading.client.dto.OrderSide
import com.tripleauth.nexttrading.client.dto.OrderType
import com.tripleauth.nexttrading.market.MarketCalendarService
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import java.math.BigDecimal

/**
 * 실제 모의투자 서버 대상 스모크 테스트.
 * NEXT_CLIENT_ID / NEXT_CLIENT_SECRET / NEXT_ACCOUNT_ID 환경변수가 있을 때만 실행된다.
 */
@EnabledIfEnvironmentVariable(named = "NEXT_CLIENT_ID", matches = ".+")
class RealApiSmokeTest {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE, true)

    private val properties = NextApiProperties(
        clientId = System.getenv("NEXT_CLIENT_ID") ?: "",
        clientSecret = System.getenv("NEXT_CLIENT_SECRET") ?: "",
        accountId = System.getenv("NEXT_ACCOUNT_ID") ?: "acc_main",
    )

    private val client = NextApiClient(properties, TokenManager(properties, objectMapper), objectMapper)

    @Test
    fun `시세-캘린더-계좌-주문 전 구간 스모크`() {
        val quotes = client.getQuotes(listOf("AAPL", "TSLA"))
        assertThat(quotes.quotes).hasSize(2)

        val candles = client.getCandles("AAPL", CandleInterval.DAY_1, 5)
        assertThat(candles.candles).isNotEmpty()

        val calendarService = MarketCalendarService(client)
        val open = calendarService.isRegularOpen()
        println("regular market open now = $open")

        val account = client.getAccount()
        assertThat(account.accountId).isNotBlank()

        client.getHoldings()
        assertThat(client.getBuyingPower().buyingPower).isPositive()

        // 시장가와 먼 지정가 매수 → 조회 → 취소
        val order = client.createOrder(
            CreateOrderRequest(
                symbol = "AAPL",
                side = OrderSide.BUY,
                orderType = OrderType.LIMIT,
                quantity = BigDecimal.ONE,
                limitPrice = BigDecimal("150"),
                clientOrderId = "core-smoke-${System.currentTimeMillis()}",
            ),
        )
        assertThat(order.orderId).isNotBlank()

        val detail = client.getOrder(order.orderId)
        assertThat(detail.orderId).isEqualTo(order.orderId)

        val canceled = client.cancelOrder(order.orderId)
        assertThat(canceled.canceledAt).isNotNull()

        client.getFills()
        println("smoke ok / orderId=${order.orderId} canceledAt=${canceled.canceledAt}")
    }
}
