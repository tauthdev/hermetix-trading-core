package com.tripleauth.hermetix.engine

import com.tripleauth.hermetix.Fixtures
import com.tripleauth.hermetix.broker.BrokerCapabilities
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.MarketStream
import com.tripleauth.hermetix.broker.OrderBookLevel
import com.tripleauth.hermetix.broker.OrderBookListener
import com.tripleauth.hermetix.broker.OrderBookTick
import com.tripleauth.hermetix.broker.OrderEvent
import com.tripleauth.hermetix.broker.OrderEventListener
import com.tripleauth.hermetix.broker.OrderEventType
import com.tripleauth.hermetix.broker.StreamChannel
import com.tripleauth.hermetix.broker.StreamingBrokerClient
import com.tripleauth.hermetix.broker.TradeListener
import com.tripleauth.hermetix.broker.TradeTick
import com.tripleauth.hermetix.broker.TradingEnvironment
import com.tripleauth.hermetix.client.dto.BuyingPowerResponse
import com.tripleauth.hermetix.client.dto.CandleInterval
import com.tripleauth.hermetix.client.dto.CandlesResponse
import com.tripleauth.hermetix.client.dto.HoldingsResponse
import com.tripleauth.hermetix.client.dto.OrdersResponse
import com.tripleauth.hermetix.client.dto.QuotesResponse
import com.tripleauth.hermetix.market.MarketCalendarService
import com.tripleauth.hermetix.strategy.Signal
import com.tripleauth.hermetix.strategy.StrategyContext
import com.tripleauth.hermetix.strategy.StrategySpec
import com.tripleauth.hermetix.strategy.TickTrigger
import com.tripleauth.hermetix.strategy.TradingStrategy
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/** ON_TRADE 트리거 — 스트림 틱이 tick 을 촉발하고, 합쳐지고, 최소 간격을 지키고, 현재가를 REST 대신 틱에서 가져오는지 */
class StrategyEngineStreamTest {

    /** 연결 없이 틱을 밀어 넣을 수 있는 가짜 스트림 */
    private class FakeStream : MarketStream {
        val listeners = CopyOnWriteArrayList<Pair<List<String>, TradeListener>>()
        val bookListeners = CopyOnWriteArrayList<Pair<List<String>, OrderBookListener>>()
        val orderListeners = CopyOnWriteArrayList<OrderEventListener>()
        var closed = false
        override val isConnected: Boolean get() = !closed
        override fun connect() {}
        override fun subscribeTrades(symbols: List<String>, listener: TradeListener) { listeners += symbols to listener }
        override fun subscribeOrderBook(symbols: List<String>, listener: OrderBookListener) { bookListeners += symbols to listener }
        override fun subscribeOrderEvents(listener: OrderEventListener) { orderListeners += listener }
        override fun close() { closed = true }
        fun emitBook(symbol: String, ask: String, bid: String) {
            val tick = OrderBookTick(symbol, Instant.now(), asks = listOf(OrderBookLevel(BigDecimal(ask), BigDecimal.TEN)), bids = listOf(OrderBookLevel(BigDecimal(bid), BigDecimal.TEN)))
            bookListeners.filter { symbol in it.first }.forEach { it.second.onOrderBook(tick) }
        }
        fun emitOrderEvent(event: OrderEvent) = orderListeners.forEach { it.onOrderEvent(event) }
        fun emit(symbol: String, price: String) {
            val tick = TradeTick(symbol = symbol, price = BigDecimal(price), quantity = BigDecimal.ONE, timestamp = Instant.now(), cumulativeVolume = 10L)
            listeners.filter { symbol in it.first }.forEach { it.second.onTrade(tick) }
        }
    }

    private class RecordingStrategy(override val spec: StrategySpec) : TradingStrategy {
        val calls = CopyOnWriteArrayList<Pair<Long, StrategyContext>>()
        override fun decide(context: StrategyContext): List<Signal> {
            calls += System.currentTimeMillis() to context
            return emptyList()
        }
    }

    private var engine: StrategyEngine? = null
    private val stream = FakeStream()

    @AfterEach
    fun tearDown() {
        engine?.destroy()
    }

    private val allStreams = setOf(StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS)

    private fun caps(streams: Set<StreamChannel>) = BrokerCapabilities(
        brokerId = "test", market = "KRX", currency = "KRW",
        candleIntervals = setOf(CandleInterval.DAY_1), clientOrderId = false, nativeBracket = false, fractionalShares = false,
        streams = streams,
    )

    private fun stubData(broker: BrokerClient) {
        every { broker.environment } returns TradingEnvironment.PAPER
        every { broker.getCandles(any(), any(), any()) } answers { CandlesResponse(firstArg(), "1d", emptyList()) }
        every { broker.getAccount() } returns Fixtures.account()
        every { broker.getHoldings() } returns HoldingsResponse(emptyList())
        every { broker.getOrders() } returns OrdersResponse(emptyList())
        every { broker.getBuyingPower() } returns BuyingPowerResponse("acc", "KRW", BigDecimal("1000000"))
        every { broker.getQuotes(any()) } answers { QuotesResponse(firstArg<List<String>>().map { Fixtures.quote(it, "1") }) }
    }

    private fun streamingBroker(streams: Set<StreamChannel> = setOf(StreamChannel.TRADES)): StreamingBrokerClient = mockk<StreamingBrokerClient>().also {
        every { it.capabilities } returns caps(streams)
        every { it.openStream() } returns stream
        every { it.applyOrderEvent(any()) } returns Unit
        stubData(it)
    }

    private fun spec(symbols: List<String>, minTickInterval: Duration = Duration.ofMillis(300)) = StrategySpec(
        name = "s", symbols = symbols, pollInterval = Duration.ofHours(1), regularHoursOnly = false,
        trigger = TickTrigger.ON_TRADE, minTickInterval = minTickInterval,
    )

    private fun engine(broker: BrokerClient, strategy: TradingStrategy): StrategyEngine {
        val guard = mockk<TradingGuard>(relaxed = true).also { every { it.isHalted } returns false }
        val bracket = mockk<BracketMonitor>().also { every { it.check(any()) } returns emptyList() }
        return StrategyEngine(listOf(strategy), broker, mockk<MarketCalendarService>(), mockk(relaxed = true), bracket, guard)
            .also { engine = it }
    }

    private fun awaitCalls(strategy: RecordingStrategy, atLeast: Int, timeoutMillis: Long = 3000) {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (strategy.calls.size < atLeast && System.currentTimeMillis() < deadline) Thread.sleep(20)
        assertThat(strategy.calls.size).isGreaterThanOrEqualTo(atLeast)
    }

    @Test
    fun `틱이 오면 전략이 호출되고 현재가는 REST 대신 스트림 틱에서 온다`() {
        val broker = streamingBroker()
        val strategy = RecordingStrategy(spec(listOf("005930")))
        engine(broker, strategy).start()
        assertThat(stream.listeners.single().first).containsExactly("005930")
        awaitCalls(strategy, 1) // 스케줄러는 기동 직후 폴링 틱을 한 번 돌린다 — 이때는 스트림 틱이 없어 REST 현재가

        stream.emit("005930", "71500")
        awaitCalls(strategy, 2)

        val context = strategy.calls[1].second
        assertThat(context.quote("005930")?.price).isEqualByComparingTo("71500")
        verify(exactly = 1) { broker.getQuotes(any()) } // 기동 폴링 틱 1회뿐 — 스트림 틱은 REST 를 부르지 않는다
    }

    @Test
    fun `몰려온 틱은 하나로 합쳐지고 최소 간격을 지킨다`() {
        val broker = streamingBroker()
        val strategy = RecordingStrategy(spec(listOf("005930"), minTickInterval = Duration.ofMillis(400)))
        engine(broker, strategy).start()
        awaitCalls(strategy, 1) // 기동 폴링 틱
        Thread.sleep(450) // 최소 간격을 넘겨 다음 스트림 틱이 즉시 돌 수 있게

        repeat(20) { stream.emit("005930", "7150$it") }
        Thread.sleep(1500)
        // 20개 → 즉시 1회 + (tick 도중 도착한 틱이 있으면) 최소 간격 뒤 1회. 20회가 아니다
        val afterBurst = strategy.calls.size
        assertThat(afterBurst).isBetween(2, 3)

        stream.emit("005930", "72000")
        awaitCalls(strategy, afterBurst + 1)
        Thread.sleep(300)
        assertThat(strategy.calls.size).isEqualTo(afterBurst + 1)

        // 스트림이 촉발한 tick 사이의 간격은 항상 minTickInterval 이상
        strategy.calls.map { it.first }.zipWithNext { a, b -> b - a }.forEach { gap -> assertThat(gap).isGreaterThanOrEqualTo(380L) }
    }

    @Test
    fun `일부 심볼만 틱이 있으면 현재가는 REST 로 간다`() {
        val broker = streamingBroker()
        val strategy = RecordingStrategy(spec(listOf("005930", "000660")))
        engine(broker, strategy).start()
        awaitCalls(strategy, 1) // 기동 폴링 틱

        stream.emit("005930", "71500")
        awaitCalls(strategy, 2)

        verify(exactly = 2) { broker.getQuotes(listOf("005930", "000660")) } // 000660 틱이 없으므로 스트림 틱도 REST
    }

    @Test
    fun `스트림 미지원 브로커에서 ON_TRADE 는 폴링으로 스케줄된다`() {
        val broker = mockk<BrokerClient>().also {
            every { it.capabilities } returns caps(emptySet())
            stubData(it)
        }
        val strategy = RecordingStrategy(spec(listOf("005930")))
        val e = engine(broker, strategy)
        e.start()
        assertThat(e.scheduledStrategies).containsExactly("s")
        assertThat(e.streamConnected).isFalse()
    }

    @Test
    fun `destroy 는 스트림을 닫는다`() {
        val e = engine(streamingBroker(), RecordingStrategy(spec(listOf("005930"))))
        e.start()
        assertThat(e.streamConnected).isTrue()
        e.destroy()
        engine = null
        assertThat(stream.closed).isTrue()
    }

    @Test
    fun `orderBook 전략은 호가 스트림을 구독하고 최신 호가창이 컨텍스트로 들어간다`() {
        val broker = streamingBroker(allStreams)
        val strategy = RecordingStrategy(spec(listOf("005930")).copy(orderBook = true))
        engine(broker, strategy).start()
        assertThat(stream.bookListeners.single().first).containsExactly("005930")
        awaitCalls(strategy, 1)
        assertThat(strategy.calls[0].second.orderBook("005930")).isNull() // 아직 호가 없음

        stream.emitBook("005930", ask = "250500", bid = "250000")
        stream.emit("005930", "250500")
        awaitCalls(strategy, 2)
        val book = strategy.calls[1].second.orderBook("KRX:005930") // 접두 무시 조회
        assertThat(book?.bestAsk?.price).isEqualByComparingTo("250500")
        assertThat(book?.bestBid?.price).isEqualByComparingTo("250000")
    }

    @Test
    fun `주문 통보는 어댑터 applyOrderEvent 와 브라켓에 전달된다`() {
        val broker = streamingBroker(allStreams)
        val bracket = mockk<BracketMonitor>(relaxed = true).also { every { it.check(any()) } returns emptyList() }
        val guard = mockk<TradingGuard>(relaxed = true).also { every { it.isHalted } returns false }
        val e = StrategyEngine(listOf(RecordingStrategy(spec(listOf("005930")))), broker, mockk<MarketCalendarService>(), mockk(relaxed = true), bracket, guard).also { engine = it }
        e.start()
        assertThat(stream.orderListeners).hasSize(1)

        val event = OrderEvent(orderId = "0000012345", type = OrderEventType.FILLED, timestamp = Instant.now(), quantity = BigDecimal.ONE)
        stream.emitOrderEvent(event)
        verify(exactly = 1) { broker.applyOrderEvent(event) }
        verify(exactly = 1) { bracket.onOrderEvent(event) }
    }

    @Test
    fun `주문 통보 구독이 실패해도 엔진은 기동한다 (KIS HTS ID 미설정 등)`() {
        val broker = streamingBroker(allStreams)
        val failing = object : MarketStream by stream {
            override fun subscribeOrderEvents(listener: OrderEventListener) = throw IllegalStateException("HTS ID 필요")
        }
        every { broker.openStream() } returns failing
        val strategy = RecordingStrategy(spec(listOf("005930")))
        val e = engine(broker, strategy)
        e.start()
        assertThat(e.scheduledStrategies).containsExactly("s")
        assertThat(stream.orderListeners).isEmpty()
    }
}
