package com.hanaset.nexttrading.engine

import com.hanaset.nexttrading.Fixtures
import com.hanaset.nexttrading.client.NextApiClient
import com.hanaset.nexttrading.client.dto.OrderResponse
import com.hanaset.nexttrading.client.dto.OrderStatus
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class BracketMonitorTest {

    private val nextApiClient = mockk<NextApiClient>()
    private val monitor = BracketMonitor(nextApiClient)

    private fun entryOrder(status: OrderStatus) = OrderResponse(orderId = "ord_1", status = status)

    @Test
    fun `진입 체결 후 익절가 도달 시 Sell 시그널을 만든다`() {
        every { nextApiClient.getOrder("ord_1") } returns entryOrder(OrderStatus.FILLED)
        monitor.register("ord_1", "AAPL", BigDecimal("2"), takeProfitPrice = BigDecimal("310"), stopLossPrice = BigDecimal("280"))

        val context = Fixtures.context(
            quotes = mapOf("AAPL" to Fixtures.quote(price = "311")),
            holdings = mapOf("AAPL" to Fixtures.holding(quantity = "2")),
        )

        val signals = monitor.check(context)

        assertThat(signals).hasSize(1)
        assertThat(signals[0].symbol).isEqualTo("AAPL")
        assertThat(signals[0].quantity).isEqualByComparingTo(BigDecimal("2"))
        assertThat(monitor.activeCount()).isZero()
    }

    @Test
    fun `손절가 도달 시에도 청산하며 보유 수량으로 클램프한다`() {
        every { nextApiClient.getOrder("ord_1") } returns entryOrder(OrderStatus.FILLED)
        monitor.register("ord_1", "AAPL", BigDecimal("5"), takeProfitPrice = null, stopLossPrice = BigDecimal("290"))

        val context = Fixtures.context(
            quotes = mapOf("AAPL" to Fixtures.quote(price = "289")),
            holdings = mapOf("AAPL" to Fixtures.holding(quantity = "3")),
        )

        val signals = monitor.check(context)

        assertThat(signals).hasSize(1)
        assertThat(signals[0].quantity).isEqualByComparingTo(BigDecimal("3"))
    }

    @Test
    fun `진입 주문이 취소되면 브라켓을 폐기한다`() {
        every { nextApiClient.getOrder("ord_1") } returns entryOrder(OrderStatus.CANCELED)
        monitor.register("ord_1", "AAPL", BigDecimal("2"), takeProfitPrice = BigDecimal("310"), stopLossPrice = null)

        val signals = monitor.check(Fixtures.context())

        assertThat(signals).isEmpty()
        assertThat(monitor.activeCount()).isZero()
    }

    @Test
    fun `진입이 미체결이면 청산하지 않는다`() {
        every { nextApiClient.getOrder("ord_1") } returns entryOrder(OrderStatus.SUBMITTED)
        monitor.register("ord_1", "AAPL", BigDecimal("2"), takeProfitPrice = BigDecimal("310"), stopLossPrice = null)

        val context = Fixtures.context(quotes = mapOf("AAPL" to Fixtures.quote(price = "999")))

        assertThat(monitor.check(context)).isEmpty()
        assertThat(monitor.activeCount()).isEqualTo(1)
    }
}
