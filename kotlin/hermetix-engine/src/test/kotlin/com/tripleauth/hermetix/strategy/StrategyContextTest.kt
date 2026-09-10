package com.tripleauth.hermetix.strategy

import com.tripleauth.hermetix.Fixtures
import com.tripleauth.hermetix.client.dto.OrderResponse
import com.tripleauth.hermetix.client.dto.OrderStatus
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class StrategyContextTest {

    @Test
    fun `시장 접두 유무를 무시하고 코드로 조회한다`() {
        val context = Fixtures.context(
            quotes = mapOf("KRX:005930" to Fixtures.quote(symbol = "KRX:005930", price = "70000")),
            holdings = mapOf("005930" to Fixtures.holding(symbol = "005930", quantity = "3")),
            openOrders = listOf(
                OrderResponse(orderId = "o1", status = OrderStatus.SUBMITTED, symbol = "005930"),
                OrderResponse(orderId = "o2", status = OrderStatus.SUBMITTED, symbol = "KRX:000660"),
            ),
        )

        assertThat(context.quote("005930")?.price).isEqualByComparingTo("70000")
        assertThat(context.quote("KRX:005930")?.price).isEqualByComparingTo("70000")
        assertThat(context.holding("KRX:005930")?.quantity).isEqualByComparingTo("3")
        assertThat(context.hasPosition("KRX:005930")).isTrue()
        assertThat(context.hasOpenOrder("KRX:005930")).isTrue()
        assertThat(context.hasOpenOrder("US:005930")).isTrue()   // 접두 없는 쪽은 어느 시장과도 맞는다 (단일 시장 어댑터)
        assertThat(context.hasOpenOrder("000660")).isTrue()
        assertThat(context.hasOpenOrder("US:000660")).isFalse()  // 둘 다 시장을 명시하면 달라야 불일치
        assertThat(context.quote("AAPL")).isNull()
    }
}
