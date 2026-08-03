package com.tripleauth.nexttrading.engine

import com.tripleauth.nexttrading.client.NextApiClient
import com.tripleauth.nexttrading.market.MarketCalendarService
import com.tripleauth.nexttrading.strategy.StrategyContext
import com.tripleauth.nexttrading.strategy.TradingStrategy
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import java.time.ZonedDateTime

/**
 * 전략 실행 엔진.
 *
 * 등록된 모든 [TradingStrategy] 빈을 각자의 pollInterval 주기로 호출한다.
 * 매 틱: 장시간 확인 → 시장/계좌 스냅샷 구성 → 브라켓 점검 → 전략 호출 → 시그널 실행.
 */
class StrategyEngine(
    private val strategies: List<TradingStrategy>,
    private val nextApiClient: NextApiClient,
    private val marketCalendarService: MarketCalendarService,
    private val orderExecutor: OrderExecutor,
    private val bracketMonitor: BracketMonitor,
    private val tradingGuard: TradingGuard,
) : DisposableBean {

    private val logger = KotlinLogging.logger { }

    private val scheduler = ThreadPoolTaskScheduler().apply {
        poolSize = 1
        setThreadNamePrefix("strategy-engine-")
        initialize()
    }

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        if (strategies.isEmpty()) {
            logger.warn { "등록된 TradingStrategy 빈이 없습니다 - 엔진을 시작하지 않습니다" }
            return
        }

        strategies.forEach { strategy ->
            logger.info { "strategy scheduled / ${strategy.spec.name} symbols=${strategy.spec.symbols} interval=${strategy.spec.pollInterval}" }
            scheduler.scheduleWithFixedDelay({ tick(strategy) }, strategy.spec.pollInterval)
        }
    }

    internal fun tick(strategy: TradingStrategy) {
        val spec = strategy.spec
        try {
            if (tradingGuard.isHalted) return

            if (spec.regularHoursOnly && !marketCalendarService.isRegularOpen()) {
                logger.debug { "[${spec.name}] market closed - tick skipped" }
                return
            }

            val context = buildContext(strategy)

            // 전략 호출 전에 브라켓부터 점검한다 (익절/손절이 전략 판단보다 우선)
            val bracketSignals = bracketMonitor.check(context)
            if (bracketSignals.isNotEmpty()) {
                orderExecutor.execute(spec.name, bracketSignals, context)
            }

            val signals = strategy.decide(context)
            if (signals.isNotEmpty()) {
                orderExecutor.execute(spec.name, signals, context)
            }

            tradingGuard.recordSuccess()
        } catch (e: Exception) {
            logger.error(e) { "[${spec.name}] tick failed" }
            tradingGuard.recordFailure(e)
        }
    }

    private fun buildContext(strategy: TradingStrategy): StrategyContext {
        val spec = strategy.spec

        val quotes = nextApiClient.getQuotes(spec.symbols).quotes.associateBy { it.symbol }
        val candles = spec.symbols.associateWith { symbol ->
            nextApiClient.getCandles(symbol, spec.candleInterval, spec.candleLimit).candles
        }
        val account = nextApiClient.getAccount()
        val holdings = nextApiClient.getHoldings().holdings.associateBy { it.symbol }
        val openOrders = nextApiClient.getOrders().orders.filter { it.status.isOpen }
        val buyingPower = nextApiClient.getBuyingPower().buyingPower

        return StrategyContext(
            now = ZonedDateTime.now(),
            quotes = quotes,
            candles = candles,
            account = account,
            holdings = holdings,
            openOrders = openOrders,
            buyingPower = buyingPower,
        )
    }

    override fun destroy() {
        scheduler.shutdown()
    }
}
