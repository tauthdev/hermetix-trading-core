package com.tripleauth.hermetix.engine

import com.tripleauth.hermetix.Fixtures
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.OrderEvent
import com.tripleauth.hermetix.broker.OrderEventType
import com.tripleauth.hermetix.client.dto.OrderResponse
import com.tripleauth.hermetix.client.dto.OrderStatus
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

class BracketMonitorTest {

    private val brokerClient = mockk<BrokerClient>()
    private val monitor = BracketMonitor(brokerClient)

    private fun entryOrder(status: OrderStatus) = OrderResponse(orderId = "ord_1", status = status)

    @Test
    fun `진입 체결 후 익절가 도달 시 Sell 시그널을 만든다`() {
        every { brokerClient.getOrder("ord_1") } returns entryOrder(OrderStatus.FILLED)
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
        every { brokerClient.getOrder("ord_1") } returns entryOrder(OrderStatus.FILLED)
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
        every { brokerClient.getOrder("ord_1") } returns entryOrder(OrderStatus.CANCELED)
        monitor.register("ord_1", "AAPL", BigDecimal("2"), takeProfitPrice = BigDecimal("310"), stopLossPrice = null)

        val signals = monitor.check(Fixtures.context())

        assertThat(signals).isEmpty()
        assertThat(monitor.activeCount()).isZero()
    }

    @Test
    fun `진입이 미체결이면 청산하지 않는다`() {
        every { brokerClient.getOrder("ord_1") } returns entryOrder(OrderStatus.SUBMITTED)
        monitor.register("ord_1", "AAPL", BigDecimal("2"), takeProfitPrice = BigDecimal("310"), stopLossPrice = null)

        val context = Fixtures.context(quotes = mapOf("AAPL" to Fixtures.quote(price = "999")))

        assertThat(monitor.check(context)).isEmpty()
        assertThat(monitor.activeCount()).isEqualTo(1)
    }

    private fun event(type: OrderEventType, orderId: String = "0000ord_1", quantity: String? = null) =
        OrderEvent(orderId = orderId, type = type, timestamp = Instant.now(), quantity = quantity?.let { BigDecimal(it) })

    @Test
    fun `주문 통보로 체결이 누적되어 주문 수량을 채우면 서버 조회 없이 활성화된다`() {
        monitor.register("ord_1", "AAPL", BigDecimal("2"), takeProfitPrice = BigDecimal("310"), stopLossPrice = null)
        monitor.onOrderEvent(event(OrderEventType.ACCEPTED))
        monitor.onOrderEvent(event(OrderEventType.FILLED, quantity = "1")) // 부분 체결 — 아직 비활성
        val partial = Fixtures.context(quotes = mapOf("AAPL" to Fixtures.quote(price = "311")), holdings = mapOf("AAPL" to Fixtures.holding(quantity = "2")))
        every { brokerClient.getOrder("ord_1") } returns entryOrder(OrderStatus.PARTIALLY_FILLED)
        assertThat(monitor.check(partial)).isEmpty()

        monitor.onOrderEvent(event(OrderEventType.FILLED, quantity = "1")) // 누적 2 = 주문 수량 → 활성
        io.mockk.clearMocks(brokerClient)
        val signals = monitor.check(partial) // getOrder 를 부르지 않는다 (mock 이 비어 있어도 통과)
        assertThat(signals).hasSize(1)
    }

    @Test
    fun `주문 통보로 취소·거부되면 브라켓을 폐기한다`() {
        monitor.register("ord_1", "AAPL", BigDecimal("2"), takeProfitPrice = BigDecimal("310"), stopLossPrice = null)
        monitor.onOrderEvent(event(OrderEventType.CANCELED))
        assertThat(monitor.activeCount()).isZero()

        monitor.register("ord_2", "AAPL", BigDecimal("2"), takeProfitPrice = BigDecimal("310"), stopLossPrice = null)
        monitor.onOrderEvent(event(OrderEventType.REJECTED, orderId = "ord_2"))
        assertThat(monitor.activeCount()).isZero()

        monitor.register("ord_3", "AAPL", BigDecimal("2"), takeProfitPrice = BigDecimal("310"), stopLossPrice = null)
        monitor.onOrderEvent(event(OrderEventType.CANCELED, orderId = "other")) // 다른 주문 — 무시
        assertThat(monitor.activeCount()).isEqualTo(1)
    }
}
