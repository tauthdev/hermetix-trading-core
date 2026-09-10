package com.tripleauth.hermetix.engine

import com.tripleauth.hermetix.broker.BrokerCapabilities
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.TradingEnvironment
import com.tripleauth.hermetix.client.dto.CandleInterval
import com.tripleauth.hermetix.market.MarketCalendarService
import com.tripleauth.hermetix.strategy.Signal
import com.tripleauth.hermetix.strategy.StrategyContext
import com.tripleauth.hermetix.strategy.StrategySpec
import com.tripleauth.hermetix.strategy.TradingStrategy
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration

/** 기동 시 환경·실전 게이트 검증 (fail-fast) */
class StrategyEngineTest {

    private val strategy = object : TradingStrategy {
        override val spec = StrategySpec(name = "t", symbols = listOf("AAPL"), pollInterval = Duration.ofHours(1))
        override fun decide(context: StrategyContext): List<Signal> = emptyList()
    }

    private var engine: StrategyEngine? = null

    @AfterEach
    fun tearDown() {
        engine?.destroy()
    }

    private fun broker(environment: TradingEnvironment, supports: Set<TradingEnvironment>): BrokerClient = mockk {
        every { capabilities } returns BrokerCapabilities(
            brokerId = "test", market = "US", currency = "USD",
            candleIntervals = setOf(CandleInterval.DAY_1), clientOrderId = true, nativeBracket = false, fractionalShares = false,
            environments = supports,
        )
        every { this@mockk.environment } returns environment
    }

    private fun engine(broker: BrokerClient, liveEnabled: Boolean): StrategyEngine =
        StrategyEngine(
            listOf(strategy), broker, mockk<MarketCalendarService>(), mockk<OrderExecutor>(), mockk<BracketMonitor>(), mockk<TradingGuard>(),
            liveTradingEnabled = liveEnabled,
        ).also { engine = it }

    @Test
    fun `모의 환경은 동의 없이 스케줄된다`() {
        val e = engine(broker(TradingEnvironment.PAPER, setOf(TradingEnvironment.PAPER)), liveEnabled = false)
        e.start()
        assertThat(e.scheduledStrategies).containsExactly("t")
    }

    @Test
    fun `실전 환경은 hermetix_live_enabled 없이는 스케줄되지 않는다`() {
        val e = engine(broker(TradingEnvironment.LIVE, setOf(TradingEnvironment.PAPER, TradingEnvironment.LIVE)), liveEnabled = false)
        e.start()
        assertThat(e.scheduledStrategies).isEmpty()
    }

    @Test
    fun `실전 환경 + 명시 동의면 스케줄된다`() {
        val e = engine(broker(TradingEnvironment.LIVE, setOf(TradingEnvironment.PAPER, TradingEnvironment.LIVE)), liveEnabled = true)
        e.start()
        assertThat(e.scheduledStrategies).containsExactly("t")
    }

    @Test
    fun `브로커가 선언하지 않은 환경이면 스케줄되지 않는다`() {
        val e = engine(broker(TradingEnvironment.LIVE, setOf(TradingEnvironment.PAPER)), liveEnabled = true)
        e.start()
        assertThat(e.scheduledStrategies).isEmpty()
    }
}
