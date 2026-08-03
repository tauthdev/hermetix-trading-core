package com.tripleauth.nexttrading.pnl

import com.tripleauth.nexttrading.Fixtures
import com.tripleauth.nexttrading.client.NextApiClient
import com.tripleauth.nexttrading.client.dto.Holding
import com.tripleauth.nexttrading.client.dto.HoldingsResponse
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class PnlServiceTest {

    private val nextApiClient = mockk<NextApiClient>()

    private fun holding(symbol: String, marketValue: String?, pnl: String?) = Holding(
        symbol = symbol,
        quantity = BigDecimal.ONE,
        avgEntryPrice = BigDecimal("100"),
        currentPrice = null,
        marketValue = marketValue?.let { BigDecimal(it) },
        unrealizedPnl = pnl?.let { BigDecimal(it) },
        unrealizedPnlRate = null,
    )

    @Test
    fun `보유 평가액과 평가손익을 합산한다`() {
        every { nextApiClient.getAccount() } returns Fixtures.account() // portfolioValue=20000
        every { nextApiClient.getHoldings() } returns HoldingsResponse(
            holdings = listOf(
                holding("AAPL", "600", "50"),
                holding("MSFT", "900", "-20"),
                holding("NVDA", null, null), // null 필드는 0으로 취급
            ),
        )

        val report = PnlService(nextApiClient, initialCapital = null).report()

        assertThat(report.totalMarketValue).isEqualByComparingTo(BigDecimal("1500"))
        assertThat(report.totalUnrealizedPnl).isEqualByComparingTo(BigDecimal("30"))
        assertThat(report.totalReturnRate).isNull()
        assertThat(report.holdings).hasSize(3)
    }

    @Test
    fun `시작 자금이 설정되면 총수익률을 계산한다`() {
        every { nextApiClient.getAccount() } returns Fixtures.account() // portfolioValue=20000
        every { nextApiClient.getHoldings() } returns HoldingsResponse(holdings = emptyList())

        val report = PnlService(nextApiClient, initialCapital = BigDecimal("18000")).report()

        // (20000 - 18000) / 18000 = 0.111111
        assertThat(report.totalReturnRate).isEqualByComparingTo(BigDecimal("0.111111"))
    }
}
