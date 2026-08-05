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
import com.tripleauth.hermetix.client.kis.KisApiClient
import com.tripleauth.hermetix.client.kis.KisApiProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import java.math.BigDecimal

/**
 * KIS 모의투자 실서버 스모크 테스트.
 * KIS_APPKEY / KIS_APPSECRET / KIS_CANO 환경변수가 있을 때만 실행된다.
 */
@EnabledIfEnvironmentVariable(named = "KIS_APPKEY", matches = ".+")
class KisRealApiSmokeTest {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    private val client = KisApiClient(
        KisApiProperties(
            appkey = System.getenv("KIS_APPKEY") ?: "",
            appsecret = System.getenv("KIS_APPSECRET") ?: "",
            cano = System.getenv("KIS_CANO") ?: "",
        ),
        objectMapper,
    )

    @Test
    fun `시세-캔들-캘린더-계좌-주문 전 구간 스모크`() {
        // 시세 (삼성전자)
        val quotes = client.getQuotes(listOf("005930"))
        assertThat(quotes.quotes).hasSize(1)
        assertThat(quotes.quotes[0].price).isPositive()
        println("현재가: ${quotes.quotes[0].price} / 등락률: ${quotes.quotes[0].changeRate}")

        // 일봉
        val candles = client.getCandles("005930", CandleInterval.DAY_1, 30)
        assertThat(candles.candles).hasSizeGreaterThanOrEqualTo(20)
        assertThat(candles.candles.first().timestamp).isBefore(candles.candles.last().timestamp) // 과거→최신
        println("일봉 ${candles.candles.size}개 / 최신 종가: ${candles.candles.last().close}")

        // 캘린더 (KRX 합성)
        val calendar = client.getCalendar()
        println("캘린더 오늘: ${calendar.calendar.first().date} open=${calendar.calendar.first().open}")

        // 계좌/보유/매수가능
        val account = client.getAccount()
        assertThat(account.currency).isEqualTo("KRW")
        assertThat(account.cash).isPositive()
        println("예수금: ${account.cash} / 총평가: ${account.portfolioValue}")

        val holdings = client.getHoldings()
        println("보유: ${holdings.holdings.map { "${it.symbol} x${it.quantity} (pnl=${it.unrealizedPnl})" }}")

        assertThat(client.getBuyingPower().buyingPower).isPositive()

        // 주문 목록/체결
        client.getOrders()
        client.getFills()

        // 주문 생성→취소 (장중일 때만 가능 — 장외면 거부 메시지 확인으로 대체)
        try {
            val order = client.createOrder(
                CreateOrderRequest(
                    symbol = "005930",
                    side = OrderSide.BUY,
                    orderType = OrderType.LIMIT,
                    quantity = BigDecimal.ONE,
                    limitPrice = quotes.quotes[0].price.multiply(BigDecimal("0.8")).setScale(-2, java.math.RoundingMode.DOWN),
                ),
            )
            assertThat(order.orderId).isNotBlank()
            println("주문 접수: ${order.orderId}")

            val canceled = client.cancelOrder(order.orderId)
            assertThat(canceled.status.name).isEqualTo("CANCELED")
            println("주문 취소 완료")
        } catch (e: BrokerApiException) {
            // 장외 시간이면 KIS 가 거부한다 — 에러 경로가 정상 동작함을 확인
            println("주문 스킵 (장외): ${e.message}")
            assertThat(e.message).contains("KIS")
        }
    }
}
