package com.tripleauth.hermetix

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.BrokerApiException
import com.tripleauth.hermetix.client.dto.CandleInterval
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderType
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiClient
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import java.math.BigDecimal

/**
 * 키움 REST 모의투자 실서버 스모크 테스트.
 * KIWOOM_APPKEY / KIWOOM_SECRETKEY 환경변수가 있을 때만 실행된다.
 */
@EnabledIfEnvironmentVariable(named = "KIWOOM_APPKEY", matches = ".+")
class KiwoomRealApiSmokeTest {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    private val client = KiwoomApiClient(
        KiwoomApiProperties(
            appkey = System.getenv("KIWOOM_APPKEY") ?: "",
            secretkey = System.getenv("KIWOOM_SECRETKEY") ?: "",
        ),
        objectMapper,
    )

    @Test
    fun `시세-캔들-캘린더-계좌-주문 전 구간 스모크`() {
        val quotes = client.getQuotes(listOf("005930"))
        assertThat(quotes.quotes).hasSize(1)
        assertThat(quotes.quotes[0].price).isPositive() // 부호 제거 확인
        println("현재가: ${quotes.quotes[0].price} / 등락률: ${quotes.quotes[0].changeRate}")

        val candles = client.getCandles("005930", CandleInterval.DAY_1, 30)
        assertThat(candles.candles).hasSize(30)
        assertThat(candles.candles.first().timestamp).isBefore(candles.candles.last().timestamp)
        assertThat(candles.candles.last().close).isPositive()
        println("일봉 ${candles.candles.size}개 / 최신 종가: ${candles.candles.last().close}")

        val calendar = client.getCalendar()
        println("캘린더 오늘: ${calendar.calendar.first().date} open=${calendar.calendar.first().open}")

        val account = client.getAccount()
        assertThat(account.currency).isEqualTo("KRW")
        assertThat(account.cash).isPositive() // zero-padded 파싱 확인
        println("예수금: ${account.cash} / 총평가: ${account.portfolioValue}")

        val holdings = client.getHoldings()
        println("보유: ${holdings.holdings.map { "${it.symbol} x${it.quantity}" }}")

        assertThat(client.getBuyingPower().buyingPower).isPositive()

        client.getOrders()
        client.getFills()

        try {
            val order = client.createOrder(
                CreateOrderRequest(
                    symbol = "005930",
                    side = OrderSide.BUY,
                    orderType = OrderType.LIMIT,
                    quantity = BigDecimal.ONE,
                    limitPrice = BigDecimal("200000"),
                ),
            )
            assertThat(order.orderId).isNotBlank()
            println("주문 접수: ${order.orderId}")

            val canceled = client.cancelOrder(order.orderId)
            println("주문 취소: ${canceled.status}")
        } catch (e: BrokerApiException) {
            println("주문 스킵 (장외): ${e.message}")
            assertThat(e.message).contains("키움")
        }
    }
}
