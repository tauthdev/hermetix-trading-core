"""ON_TRADE 트리거 - 스트림 틱이 tick 을 촉발하고, 합쳐지고, 최소 간격을 지키고, 현재가를 REST 대신 틱에서 가져오는지
(Kotlin StrategyEngineStreamTest 와 동일 시나리오)."""
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal

from hermetix import Account, BrokerCapabilities, CandleInterval, Quote, StreamChannel, TradeTick, TradingEnvironment
from hermetix.broker import MarketStream, StreamingBrokerClient
from hermetix.engine import StrategyEngine
from hermetix.strategy import Strategy, StrategyContext, StrategySpec, TickTrigger

from test_engine import FakeBroker


class FakeStream(MarketStream):
    """연결 없이 틱을 밀어 넣을 수 있는 가짜 스트림."""

    def __init__(self):
        self.listeners: list[tuple[list[str], object]] = []
        self.closed = False

    @property
    def is_connected(self) -> bool:
        return not self.closed

    def connect(self) -> None:
        pass

    def subscribe_trades(self, symbols, listener) -> None:
        self.listeners.append((list(symbols), listener))

    def close(self) -> None:
        self.closed = True

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

    def __init__(self, stream: FakeStream):
        super().__init__()
        self.stream = stream
        self.quote_calls: list[list[str]] = []

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
