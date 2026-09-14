"""ON_TRADE 트리거 - 스트림 틱이 tick 을 촉발하고, 합쳐지고, 최소 간격을 지키고, 현재가를 REST 대신 틱에서 가져오는지
(Kotlin StrategyEngineStreamTest 와 동일 시나리오)."""
import threading
import time
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

from hermetix import (Account, BrokerCapabilities, CandleInterval, OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType,
                      Quote, StreamChannel, TradeTick, TradingEnvironment)
from hermetix.engine import BracketMonitor
from hermetix.broker import MarketStream, StreamingBrokerClient
from hermetix.engine import StrategyEngine
from hermetix.strategy import Strategy, StrategyContext, StrategySpec, TickTrigger

from test_engine import FakeBroker


class FakeStream(MarketStream):
    """연결 없이 틱을 밀어 넣을 수 있는 가짜 스트림."""

    def __init__(self):
        self.listeners: list[tuple[list[str], object]] = []
        self.book_listeners: list[tuple[list[str], object]] = []
        self.order_listeners: list = []
        self.closed = False

    @property
    def is_connected(self) -> bool:
        return not self.closed

    def connect(self) -> None:
        pass

    def subscribe_trades(self, symbols, listener) -> None:
        self.listeners.append((list(symbols), listener))

    def subscribe_order_book(self, symbols, listener) -> None:
        self.book_listeners.append((list(symbols), listener))

    def subscribe_order_events(self, listener) -> None:
        self.order_listeners.append(listener)

    def close(self) -> None:
        self.closed = True

    def emit_book(self, symbol: str, ask: str, bid: str) -> None:
        tick = OrderBookTick(symbol, datetime.now(timezone.utc),
                             asks=[OrderBookLevel(Decimal(ask), Decimal(10))], bids=[OrderBookLevel(Decimal(bid), Decimal(10))])
        for symbols, listener in self.book_listeners:
            if symbol in symbols:
                listener(tick)

    def emit_order_event(self, event: OrderEvent) -> None:
        for listener in self.order_listeners:
            listener(event)

    def emit(self, symbol: str, price: str) -> None:
        tick = TradeTick(symbol=symbol, price=Decimal(price), quantity=Decimal(1),
                         timestamp=datetime.now(timezone.utc), cumulative_volume=10)
        for symbols, listener in self.listeners:
            if symbol in symbols:
                listener(tick)


class StreamingFakeBroker(FakeBroker, StreamingBrokerClient):
    capabilities = BrokerCapabilities(
        broker_id="fake-stream", market="KRX", currency="KRW",
        candle_intervals=frozenset({CandleInterval.DAY_1}),
        client_order_id=False, native_bracket=False, fractional_shares=False,
        streams=frozenset({StreamChannel.TRADES}))

    def __init__(self, stream: FakeStream, streams=frozenset({StreamChannel.TRADES})):
        super().__init__()
        self.stream = stream
        self.quote_calls: list[list[str]] = []
        self.applied: list[OrderEvent] = []
        self.capabilities = replace(type(self).capabilities, streams=frozenset(streams))

    def apply_order_event(self, event):
        self.applied.append(event)

    def open_stream(self):
        return self.stream

    def get_quotes(self, symbols):
        self.quote_calls.append(list(symbols))
        return [Quote(s, Decimal(1), None, None, 0, None, None, datetime.now(timezone.utc)) for s in symbols]

    def get_account(self):
        return Account("acc", "KRW", Decimal(1_000_000), Decimal(1_000_000))


class Recording(Strategy):
    def __init__(self, spec: StrategySpec):
        self.spec = spec
        self.calls: list[tuple[float, StrategyContext]] = []

    def decide(self, ctx):
        self.calls.append((time.monotonic(), ctx))
        return []


def spec(symbols, min_interval=0.3):
    return StrategySpec(name="s", symbols=symbols, poll_interval_seconds=3600, regular_hours_only=False,
                        trigger=TickTrigger.ON_TRADE, min_tick_interval_seconds=min_interval)


def run_in_thread(engine: StrategyEngine) -> threading.Thread:
    t = threading.Thread(target=engine.run, daemon=True)
    t.start()
    return t


def await_calls(strategy: Recording, at_least: int, timeout: float = 3.0):
    deadline = time.monotonic() + timeout
    while len(strategy.calls) < at_least and time.monotonic() < deadline:
        time.sleep(0.02)
    assert len(strategy.calls) >= at_least


def test_stream_tick_triggers_decide_with_stream_quote():
    stream = FakeStream()
    broker = StreamingFakeBroker(stream)
    strategy = Recording(spec(["005930"]))
    engine = StrategyEngine(broker, [strategy])
    assert stream.listeners[0][0] == ["005930"]
    run_in_thread(engine)
    try:
        await_calls(strategy, 1)  # 기동 폴링 틱 - 스트림 틱이 없어 REST 현재가
        stream.emit("005930", "71500")
        await_calls(strategy, 2)
        assert strategy.calls[1][1].quote("005930").price == Decimal(71500)
        assert broker.quote_calls == [["005930"]]  # 기동 폴링 틱 1회뿐
    finally:
        engine.stop()


def test_burst_is_coalesced_and_min_interval_respected():
    stream = FakeStream()
    broker = StreamingFakeBroker(stream)
    strategy = Recording(spec(["005930"], min_interval=0.4))
    engine = StrategyEngine(broker, [strategy])
    run_in_thread(engine)
    try:
        await_calls(strategy, 1)
        time.sleep(0.45)
        for i in range(20):
            stream.emit("005930", f"7150{i}")
        time.sleep(1.5)
        after_burst = len(strategy.calls)
        assert 2 <= after_burst <= 3  # 즉시 1회 + (tick 도중 도착분) 최소 간격 뒤 1회. 20회가 아니다

        stream.emit("005930", "72000")
        await_calls(strategy, after_burst + 1)
        time.sleep(0.3)
        assert len(strategy.calls) == after_burst + 1
        gaps = [b - a for (a, _), (b, _) in zip(strategy.calls, strategy.calls[1:])]
        assert all(g >= 0.36 for g in gaps), gaps  # 0.9 × 최소 간격
    finally:
        engine.stop()


def test_partial_symbol_coverage_falls_back_to_rest_quotes():
    stream = FakeStream()
    broker = StreamingFakeBroker(stream)
    strategy = Recording(spec(["005930", "000660"]))
    engine = StrategyEngine(broker, [strategy])
    run_in_thread(engine)
    try:
        await_calls(strategy, 1)
        stream.emit("005930", "71500")
        await_calls(strategy, 2)
        assert broker.quote_calls == [["005930", "000660"], ["005930", "000660"]]
    finally:
        engine.stop()


def test_on_trade_without_streaming_broker_still_polls():
    broker = FakeBroker()
    broker.get_account = lambda: Account("acc", "USD", Decimal(1), Decimal(1))
    strategy = Recording(StrategySpec(name="s", symbols=["AAPL"], poll_interval_seconds=3600, regular_hours_only=False,
                                      trigger=TickTrigger.ON_TRADE))
    engine = StrategyEngine(broker, [strategy])
    assert [s.spec.name for s in engine.strategies] == ["s"]
    assert not engine.stream_connected
    run_in_thread(engine)
    try:
        await_calls(strategy, 1)
    finally:
        engine.stop()


def test_stop_closes_stream():
    stream = FakeStream()
    engine = StrategyEngine(StreamingFakeBroker(stream), [Recording(spec(["005930"]))])
    assert engine.stream_connected
    engine.stop()
    assert stream.closed


ALL_STREAMS = frozenset({StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS})


def test_order_book_spec_subscribes_and_latest_book_reaches_context():
    stream = FakeStream()
    broker = StreamingFakeBroker(stream, ALL_STREAMS)
    strategy = Recording(replace(spec(["005930"]), order_book=True))
    engine = StrategyEngine(broker, [strategy])
    assert stream.book_listeners[0][0] == ["005930"]
    run_in_thread(engine)
    try:
        await_calls(strategy, 1)
        assert strategy.calls[0][1].order_book("005930") is None  # 아직 호가 없음
        stream.emit_book("005930", ask="250500", bid="250000")
        stream.emit("005930", "250500")
        await_calls(strategy, 2)
        book = strategy.calls[1][1].order_book("KRX:005930")  # 접두 무시 조회
        assert book.best_ask.price == Decimal(250500) and book.best_bid.price == Decimal(250000)
    finally:
        engine.stop()


def test_order_events_reach_broker_and_brackets():
    stream = FakeStream()
    broker = StreamingFakeBroker(stream, ALL_STREAMS)
    engine = StrategyEngine(broker, [Recording(spec(["005930"]))])
    try:
        assert len(stream.order_listeners) == 1
        engine.brackets.register("0012345", "005930", Decimal(1), take_profit=Decimal(260000), stop_loss=None)
        event = OrderEvent(order_id="0000012345", type=OrderEventType.FILLED, timestamp=datetime.now(timezone.utc),
                           quantity=Decimal(1))
        stream.emit_order_event(event)
        assert broker.applied == [event]
        assert engine.brackets._brackets["0012345"].active  # 통보로 활성화 (서버 조회 없이)
    finally:
        engine.stop()


def test_order_event_subscribe_failure_does_not_stop_engine():
    class Failing(FakeStream):
        def subscribe_order_events(self, listener):
            raise ValueError("hts_id 필요")

    stream = Failing()
    engine = StrategyEngine(StreamingFakeBroker(stream, ALL_STREAMS), [Recording(spec(["005930"]))])
    try:
        assert [s.spec.name for s in engine.strategies] == ["s"]
        assert stream.order_listeners == []
    finally:
        engine.stop()


def _event(kind, order_id="0000ord_1", quantity=None):
    return OrderEvent(order_id=order_id, type=kind, timestamp=datetime.now(timezone.utc),
                      quantity=Decimal(quantity) if quantity is not None else None)


def test_bracket_activates_by_accumulated_fill_events_without_server_lookup():
    broker = FakeBroker()
    monitor = BracketMonitor(broker)
    monitor.register("ord_1", "AAPL", Decimal(2), take_profit=Decimal(310), stop_loss=None)
    monitor.on_order_event(_event(OrderEventType.ACCEPTED))
    monitor.on_order_event(_event(OrderEventType.FILLED, quantity="1"))  # 부분 체결 - 아직 비활성
    assert not monitor._brackets["ord_1"].active
    monitor.on_order_event(_event(OrderEventType.FILLED, quantity="1"))  # 누적 2 = 주문 수량 -> 활성
    assert monitor._brackets["ord_1"].active

    def boom(order_id):
        raise AssertionError("활성화된 브라켓은 서버를 조회하지 않는다")
    broker.get_order = boom
    from test_engine import ctx
    from hermetix import Holding
    quotes = {"AAPL": Quote("AAPL", Decimal(311), None, None, 0, None, None, datetime.now(timezone.utc))}
    signals = monitor.check(ctx(quotes=quotes, holdings={"AAPL": Holding("AAPL", Decimal(2), Decimal(280))}))
    assert len(signals) == 1


def test_bracket_dropped_by_cancel_or_reject_events_and_unrelated_ignored():
    monitor = BracketMonitor(FakeBroker())
    monitor.register("ord_1", "AAPL", Decimal(2), take_profit=Decimal(310), stop_loss=None)
    monitor.on_order_event(_event(OrderEventType.CANCELED))
    assert monitor.active_count == 0
    monitor.register("ord_2", "AAPL", Decimal(2), take_profit=Decimal(310), stop_loss=None)
    monitor.on_order_event(_event(OrderEventType.REJECTED, order_id="ord_2"))
    assert monitor.active_count == 0
    monitor.register("ord_3", "AAPL", Decimal(2), take_profit=Decimal(310), stop_loss=None)
    monitor.on_order_event(_event(OrderEventType.CANCELED, order_id="other"))
    assert monitor.active_count == 1
