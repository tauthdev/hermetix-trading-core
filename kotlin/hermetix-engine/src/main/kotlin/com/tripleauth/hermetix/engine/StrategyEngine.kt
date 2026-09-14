package com.tripleauth.hermetix.engine

import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.MarketStream
import com.tripleauth.hermetix.broker.OrderBookTick
import com.tripleauth.hermetix.broker.OrderEvent
import com.tripleauth.hermetix.broker.StreamChannel
import com.tripleauth.hermetix.broker.StreamingBrokerClient
import com.tripleauth.hermetix.broker.TradeTick
import com.tripleauth.hermetix.broker.MarketClosedError
import com.tripleauth.hermetix.broker.RateLimitError
import com.tripleauth.hermetix.broker.TradingEnvironment
import com.tripleauth.hermetix.market.MarketCalendarService
import com.tripleauth.hermetix.client.dto.Quote
import com.tripleauth.hermetix.strategy.StrategyContext
import com.tripleauth.hermetix.strategy.StrategySpec
import com.tripleauth.hermetix.strategy.TickTrigger
import com.tripleauth.hermetix.strategy.TradingStrategy
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import java.time.Instant
import java.time.ZonedDateTime
import java.util.concurrent.ConcurrentHashMap

/**
 * 전략 실행 엔진.
 *
 * 등록된 모든 [TradingStrategy] 빈을 각자의 pollInterval 주기로 호출한다.
 * 매 틱: 장시간 확인 → 시장/계좌 스냅샷 구성 → 브라켓 점검 → 전략 호출 → 시그널 실행.
 *
 * 실시간 트리거([TickTrigger.ON_TRADE]): 브로커가 [StreamingBrokerClient] 이고 TRADES 채널을 선언하면
 * 전략 심볼의 체결가 스트림을 구독하고, 틱마다 같은 단일 스레드 스케줄러에 tick 을 넣는다.
 * 대기 중인 틱이 있으면 합치고([pendingTicks]), 직전 틱 종료 후 minTickInterval 이 지나야 다음을 돌린다.
 * 스트림 틱이 모든 심볼을 덮으면 quotes REST 호출 대신 마지막 틱을 현재가로 쓴다.
 *
 * 실전(LIVE) 게이트: 브로커가 LIVE 환경이면 [liveTradingEnabled] 가 true 일 때만 스케줄한다.
 */
class StrategyEngine(
    private val strategies: List<TradingStrategy>,
    private val brokerClient: BrokerClient,
    private val marketCalendarService: MarketCalendarService,
    private val orderExecutor: OrderExecutor,
    private val bracketMonitor: BracketMonitor,
    private val tradingGuard: TradingGuard,
    private val liveTradingEnabled: Boolean = false,
) : DisposableBean {

    private val logger = KotlinLogging.logger { }

    /** 실제로 스케줄된 전략 이름 — 기동 검증 결과 확인용 */
    @Volatile
    var scheduledStrategies: List<String> = emptyList()
        private set

    private val scheduler = ThreadPoolTaskScheduler().apply {
        poolSize = 1
        setThreadNamePrefix("strategy-engine-")
        initialize()
    }

    /** 체결가 스트림 (ON_TRADE 전략이 하나라도 있고 브로커가 지원할 때만 연다) */
    @Volatile
    private var stream: MarketStream? = null

    /** 심볼(요청 표기) → 마지막 체결 틱 */
    private val latestTrades = ConcurrentHashMap<String, TradeTick>()

    /** 심볼(요청 표기) → 마지막 호가창 (spec.orderBook 전략만) */
    private val latestOrderBooks = ConcurrentHashMap<String, OrderBookTick>()

    /** 주문 통보 구독 여부 — 한 번만 */
    @Volatile
    private var orderEventsAttached = false

    /** 전략 이름 → 스케줄 대기 중인 스트림 틱이 있는지 (합치기용) */
    private val pendingTicks = ConcurrentHashMap<String, Boolean>()

    /** 전략 이름 → 직전 tick 종료 시각 (epoch ms) */
    private val lastTickEndedAt = ConcurrentHashMap<String, Long>()

    /** 스트림이 열려 있고 로그인까지 끝났는지 — 상태 확인용 */
    val streamConnected: Boolean get() = stream?.isConnected == true

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        if (strategies.isEmpty()) {
            logger.warn { "등록된 TradingStrategy 빈이 없습니다 - 엔진을 시작하지 않습니다" }
            return
        }

        val caps = brokerClient.capabilities
        val environment = brokerClient.environment
        logger.info { "broker=${caps.brokerId} environment=$environment market=${caps.market} candles=${caps.candleIntervals.map { it.value }}" }

        if (environment !in caps.environments) {
            logger.error { "브로커 '${caps.brokerId}' 는 $environment 환경을 지원하지 않습니다 (지원: ${caps.environments}) - 엔진을 시작하지 않습니다" }
            return
        }
        if (environment == TradingEnvironment.LIVE && !liveTradingEnabled) {
            logger.error {
                "브로커 '${caps.brokerId}' 가 실전투자(LIVE)로 설정돼 있지만 hermetix.live.enabled=true 가 없습니다 - " +
                    "엔진을 시작하지 않습니다. 실제 돈으로 거래하려면 설정에 명시하세요."
            }
            return
        }
        if (environment == TradingEnvironment.LIVE) {
            logger.warn { "***** 실전투자(LIVE) 모드 - 주문이 실제 계좌에서 체결됩니다. 주문 금액 상한(hermetix.risk.*) 설정을 권장합니다 *****" }
        }

        val scheduled = mutableListOf<String>()
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

            logger.info { "strategy scheduled / ${strategy.spec.name} symbols=${strategy.spec.symbols} interval=${strategy.spec.pollInterval} trigger=${strategy.spec.trigger}" }
            scheduler.scheduleWithFixedDelay({ tick(strategy) }, strategy.spec.pollInterval)
            if (strategy.spec.trigger == TickTrigger.ON_TRADE) attachStream(strategy)
            if (strategy.spec.orderBook) attachOrderBook(strategy)
            scheduled += strategy.spec.name
        }
        scheduledStrategies = scheduled
        attachOrderEvents()
    }

    /** 브로커가 채널을 제공하면 공유 스트림을 (필요 시 열어) 돌려주고, 아니면 null */
    private fun streamFor(channel: StreamChannel): MarketStream? {
        val broker = brokerClient
        if (broker !is StreamingBrokerClient || channel !in broker.capabilities.streams) return null
        return stream ?: broker.openStream().also { opened ->
            stream = opened
            opened.connect()
        }
    }

    /** spec.orderBook 전략의 심볼 호가창을 구독해 컨텍스트로 공급한다. 틱을 촉발하지는 않는다 */
    private fun attachOrderBook(strategy: TradingStrategy) {
        val spec = strategy.spec
        val s = streamFor(StreamChannel.ORDER_BOOK) ?: run {
            logger.warn { "[${spec.name}] orderBook=true 이지만 브로커 '${brokerClient.capabilities.brokerId}' 는 호가 스트림을 제공하지 않습니다 - 컨텍스트의 orderBook 은 비어 있습니다" }
            return
        }
        s.subscribeOrderBook(spec.symbols) { tick -> latestOrderBooks[tick.symbol] = tick }
        logger.info { "[${spec.name}] order book stream attached / symbols=${spec.symbols}" }
    }

    /**
     * 주문 통보를 구독해 어댑터 추적([StreamingBrokerClient.applyOrderEvent])과 브라켓([BracketMonitor.onOrderEvent])에 반영한다.
     * 브로커가 제공하면 항상 붙인다 — 구독 실패(KIS HTS ID 미설정 등)는 경고만 남기고 폴링 판정으로 둔다
     */
    private fun attachOrderEvents() {
        if (orderEventsAttached) return
        val broker = brokerClient as? StreamingBrokerClient ?: return
        val s = streamFor(StreamChannel.ORDER_EVENTS) ?: return
        runCatching {
            s.subscribeOrderEvents { event -> onOrderEvent(broker, event) }
            orderEventsAttached = true
            logger.info { "order event stream attached" }
        }.onFailure { logger.warn { "주문 통보 스트림을 구독하지 못했습니다 - 체결 판정은 폴링으로 계속합니다: ${it.message}" } }
    }

    private fun onOrderEvent(broker: StreamingBrokerClient, event: OrderEvent) {
        logger.info { "order event / ${event.type} order=${event.orderId} ${event.symbol ?: ""} ${event.side ?: ""} qty=${event.quantity} price=${event.price}" }
        runCatching { broker.applyOrderEvent(event) }.onFailure { logger.warn(it) { "applyOrderEvent 실패 / ${event.orderId}" } }
        runCatching { bracketMonitor.onOrderEvent(event) }.onFailure { logger.warn(it) { "bracket onOrderEvent 실패 / ${event.orderId}" } }
    }

    /** ON_TRADE 전략을 체결가 스트림에 붙인다. 브로커가 지원하지 않으면 경고만 남기고 폴링으로 둔다 */
    private fun attachStream(strategy: TradingStrategy) {
        val spec = strategy.spec
        val broker = brokerClient
        if (broker !is StreamingBrokerClient || StreamChannel.TRADES !in broker.capabilities.streams) {
            logger.warn {
                "[${spec.name}] trigger=ON_TRADE 이지만 브로커 '${broker.capabilities.brokerId}' 는 체결가 스트림을 제공하지 않습니다 - " +
                    "pollInterval=${spec.pollInterval} 폴링으로 동작합니다"
            }
            return
        }
        val s = streamFor(StreamChannel.TRADES) ?: return
        s.subscribeTrades(spec.symbols) { tick ->
            latestTrades[tick.symbol] = tick
            requestTick(strategy)
        }
        logger.info { "[${spec.name}] trade stream attached / symbols=${spec.symbols} minTickInterval=${spec.minTickInterval}" }
    }

    /**
     * 스트림 틱으로 tick 을 요청한다. 이미 대기 중이면 합친다. 스트림 스레드에서 호출되므로 스케줄만 하고 바로 돌아간다.
     * 최소 간격은 실행 직전에 다시 확인한다 — 틱이 tick 실행 도중 도착하면 스케줄 시점의 "직전 종료 시각" 이 아직 갱신 전이기 때문.
     */
    internal fun requestTick(strategy: TradingStrategy) {
        if (pendingTicks.putIfAbsent(strategy.spec.name, true) != null) return
        scheduleStreamTick(strategy)
    }

    private fun scheduleStreamTick(strategy: TradingStrategy) {
        scheduler.schedule({ runStreamTick(strategy) }, Instant.now().plusMillis(remainingInterval(strategy.spec)))
    }

    private fun runStreamTick(strategy: TradingStrategy) {
        if (remainingInterval(strategy.spec) > 0) {
            scheduleStreamTick(strategy)
            return
        }
        pendingTicks.remove(strategy.spec.name)
        tick(strategy)
    }

    /** 직전 tick 종료 후 minTickInterval 까지 남은 ms (0 이면 바로 실행 가능) */
    private fun remainingInterval(spec: StrategySpec): Long =
        ((lastTickEndedAt[spec.name] ?: 0L) + spec.minTickInterval.toMillis() - System.currentTimeMillis()).coerceAtLeast(0L)

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
        } finally {
            lastTickEndedAt[spec.name] = System.currentTimeMillis()
        }
    }

    private fun buildContext(strategy: TradingStrategy): StrategyContext {
        val spec = strategy.spec

        val quotes = streamQuotes(spec) ?: brokerClient.getQuotes(spec.symbols).quotes.associateBy { it.symbol }
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
            orderBooks = if (spec.orderBook) spec.symbols.mapNotNull { sym -> latestOrderBooks[sym]?.let { sym to it } }.toMap() else emptyMap(),
        )
    }

    /** ON_TRADE 전략의 모든 심볼에 스트림 틱이 있으면 그것을 현재가로 쓴다 (REST quotes 1회 절약). 하나라도 없으면 null → REST */
    private fun streamQuotes(spec: StrategySpec): Map<String, Quote>? {
        if (spec.trigger != TickTrigger.ON_TRADE || stream == null) return null
        val ticks = spec.symbols.map { symbol -> latestTrades[symbol] ?: return null }
        return ticks.associate { it.symbol to it.toQuote() }
    }

    override fun destroy() {
        runCatching { stream?.close() }
        scheduler.shutdown()
    }
}
