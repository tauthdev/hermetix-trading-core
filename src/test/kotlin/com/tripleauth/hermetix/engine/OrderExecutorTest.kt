package com.tripleauth.hermetix.engine

import com.tripleauth.hermetix.Fixtures
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.OrderResponse
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import com.tripleauth.hermetix.strategy.Signal
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class OrderExecutorTest {

    private val brokerClient = mockk<BrokerClient>()
    private val bracketMonitor = BracketMonitor(brokerClient)
    private val tradingGuard = mockk<TradingGuard>(relaxed = true)
    private val executor = OrderExecutor(brokerClient, bracketMonitor, tradingGuard)

    @Test
    fun `매도는 보유 수량으로 클램프된다`() {
        every { tradingGuard.isHalted } returns false
        val captured = slot<CreateOrderRequest>()
        every { brokerClient.createOrder(capture(captured)) } returns OrderResponse(orderId = "ord_1", status = OrderStatus.SUBMITTED)

        val context = Fixtures.context(holdings = mapOf("AAPL" to Fixtures.holding(quantity = "3")))
        executor.execute("test", listOf(Signal.Sell(symbol = "AAPL", quantity = BigDecimal("10"))), context)

        assertThat(captured.captured.side).isEqualTo(OrderSide.SELL)
        assertThat(captured.captured.quantity).isEqualByComparingTo(BigDecimal("3"))
        assertThat(captured.captured.clientOrderId).startsWith("test-")
    }

    @Test
    fun `보유가 없으면 매도를 스킵한다`() {
        every { tradingGuard.isHalted } returns false

        executor.execute("test", listOf(Signal.Sell(symbol = "AAPL", quantity = BigDecimal.ONE)), Fixtures.context())

        verify(exactly = 0) { brokerClient.createOrder(any()) }
    }

    @Test
    fun `비상정지 중에는 아무 시그널도 실행하지 않는다`() {
        every { tradingGuard.isHalted } returns true

        executor.execute(
            "test",
            listOf(Signal.Buy(symbol = "AAPL", quantity = BigDecimal.ONE)),
            Fixtures.context(holdings = mapOf("AAPL" to Fixtures.holding())),
        )

        verify(exactly = 0) { brokerClient.createOrder(any()) }
    }

    @Test
    fun `익절, 손절가가 있는 매수는 브라켓으로 등록된다`() {
        every { tradingGuard.isHalted } returns false
        every { brokerClient.createOrder(any()) } returns OrderResponse(orderId = "ord_9", status = OrderStatus.SUBMITTED)

        executor.execute(
            "test",
            listOf(
                Signal.Buy(
                    symbol = "AAPL",
                    quantity = BigDecimal.ONE,
                    takeProfitPrice = BigDecimal("310"),
                    stopLossPrice = BigDecimal("280"),
                ),
            ),
            Fixtures.context(),
        )

        assertThat(bracketMonitor.activeCount()).isEqualTo(1)
    }
}
