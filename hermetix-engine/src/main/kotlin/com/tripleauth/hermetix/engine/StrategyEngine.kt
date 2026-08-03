package com.tripleauth.hermetix.engine

import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.MarketClosedError
import com.tripleauth.hermetix.broker.RateLimitError
import com.tripleauth.hermetix.market.MarketCalendarService
import com.tripleauth.hermetix.strategy.StrategyContext
import com.tripleauth.hermetix.strategy.TradingStrategy
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
    private val brokerClient: BrokerClient,
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

        val caps = brokerClient.capabilities
        logger.info { "broker=${caps.brokerId} market=${caps.market} candles=${caps.candleIntervals.map { it.value }}" }

        strategies.forEach { strategy ->
            // capability 검증 — 미지원 조합은 스케줄하지 않고 명확히 알린다 (fail-fast)
            if (strategy.spec.candleInterval !in caps.candleIntervals) {
                logger.error {
                    "[${strategy.spec.name}] 스케줄 제외: 브로커 '${caps.brokerId}' 는 " +
                        "${strategy.spec.candleInterval.value} 캔들을 지원하지 않습니다 " +
                        "(지원: ${caps.candleIntervals.map { it.value }}). spec.candleInterval 을 조정하세요."
                }
                return@forEach
            }

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
        } catch (e: MarketClosedError) {
            // 휴장/장마감 — 정상 상황이므로 실패로 세지 않는다 (KRX 합성 캘린더의 공휴일 미반영 케이스 포함)
            logger.debug { "[${spec.name}] market closed - ${e.message}" }
        } catch (e: RateLimitError) {
            // 어댑터의 백오프 재시도가 소진된 경우 — 다음 틱에 자연 회복되므로 실패로 세지 않는다
            logger.warn { "[${spec.name}] rate limited - ${e.message}" }
        } catch (e: Exception) {
            logger.error(e) { "[${spec.name}] tick failed" }
            tradingGuard.recordFailure(e)
        }
    }

    private fun buildContext(strategy: TradingStrategy): StrategyContext {
        val spec = strategy.spec

        val quotes = brokerClient.getQuotes(spec.symbols).quotes.associateBy { it.symbol }
        val candles = spec.symbols.associateWith { symbol ->
            brokerClient.getCandles(symbol, spec.candleInterval, spec.candleLimit).candles
        }
        val account = brokerClient.getAccount()
        val holdings = brokerClient.getHoldings().holdings.associateBy { it.symbol }
        val openOrders = brokerClient.getOrders().orders.filter { it.status.isOpen }
        val buyingPower = brokerClient.getBuyingPower().buyingPower

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
