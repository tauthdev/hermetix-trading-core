package com.tripleauth.hermetix.broker

import com.tripleauth.hermetix.client.dto.CandleInterval
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class MarketSymbolTest {

    private val caps = BrokerCapabilities(
        brokerId = "kis", market = "KRX", currency = "KRW",
        candleIntervals = setOf(CandleInterval.DAY_1), clientOrderId = false, nativeBracket = false, fractionalShares = false,
    )

    @Test
    fun `시장 접두를 파싱하고 없으면 코드만 남긴다`() {
        assertThat(MarketSymbol.parse("KRX:005930")).isEqualTo(MarketSymbol("KRX", "005930"))
        assertThat(MarketSymbol.parse("AAPL")).isEqualTo(MarketSymbol(null, "AAPL"))
        assertThat(MarketSymbol.code("US:BRK.B")).isEqualTo("BRK.B")
        assertThat(MarketSymbol.canonical("005930", "KRX")).isEqualTo("KRX:005930")
        assertThat(MarketSymbol.parse("KRX:005930").toString()).isEqualTo("KRX:005930")
    }

    @Test
    fun `matches 는 접두 유무를 무시하고 코드로 비교하되 둘 다 명시되면 시장도 같아야 한다`() {
        assertThat(MarketSymbol.matches("KRX:005930", "005930")).isTrue()
        assertThat(MarketSymbol.matches("005930", "005930")).isTrue()
        assertThat(MarketSymbol.matches("KRX:005930", "US:005930")).isFalse()
        assertThat(MarketSymbol.matches("AAPL", "TSLA")).isFalse()
    }

    @Test
    fun `symbolCode 는 지원 시장만 통과시킨다`() {
        assertThat(caps.symbolCode("KRX:005930")).isEqualTo("005930")
        assertThat(caps.symbolCode("005930")).isEqualTo("005930")
        assertThatThrownBy { caps.symbolCode("US:AAPL") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("US")
    }

    @Test
    fun `capabilities 기본값 - 시장 목록은 기본 시장 하나, 환경은 모의만`() {
        assertThat(caps.markets).containsExactly("KRX")
        assertThat(caps.environments).containsExactly(TradingEnvironment.PAPER)
    }
}
