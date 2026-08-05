"""엔진 로직 검증 - Kotlin 레퍼런스와 동일한 시나리오."""
from datetime import datetime, timezone
from decimal import Decimal

from hermetix import (
    Account, BrokerCapabilities, Buy, CandleInterval, Holding, Order, OrderSide,
    OrderStatus, OrderType, Quote, Sell,
)
from hermetix.broker import BrokerClient
from hermetix.engine import BracketMonitor, OrderExecutor, TradingGuard
from hermetix.strategy import StrategyContext


class FakeBroker(BrokerClient):
    """테스트용 브로커 - 호출 기록 + 프로그래머블 응답."""

    capabilities = BrokerCapabilities(
        broker_id="fake", market="US", currency="USD",
        candle_intervals=frozenset({CandleInterval.DAY_1}),
        client_order_id=True, native_bracket=False, fractional_shares=False)

    def __init__(self):
        self.created = []
        self.canceled = []
        self.order_status = OrderStatus.SUBMITTED
        self.open_orders = []

    def get_quotes(self, symbols): return []
    def get_candles(self, symbol, interval, limit=None): return []
    def get_calendar(self): return []
    def get_account(self): raise NotImplementedError
    def get_holdings(self): return []
    def get_buying_power(self): return Decimal(0)

    def create_order(self, request):
        self.created.append(request)
        return Order(order_id=f"ord_{len(self.created)}", status=OrderStatus.SUBMITTED,
                     symbol=request.symbol, side=request.side, quantity=request.quantity)

    def get_orders(self): return self.open_orders
    def get_order(self, order_id): return Order(order_id=order_id, status=self.order_status)

    def cancel_order(self, order_id):
        self.canceled.append(order_id)
        return Order(order_id=order_id, status=OrderStatus.CANCELED)

    def get_fills(self): return []


def ctx(quotes=None, holdings=None):
    return StrategyContext(
        now=datetime.now(timezone.utc),
        quotes=quotes or {},
        candles={},
        account=Account("acc", "USD", Decimal(10000), Decimal(10000)),
        holdings=holdings or {},
        open_orders=[],
        buying_power=Decimal(10000),
    )


def quote(symbol, price):
    return Quote(symbol, Decimal(price), None, None, 0, None, None, datetime.now(timezone.utc))


def holding(symbol, qty):
    return Holding(symbol=symbol, quantity=Decimal(qty), avg_entry_price=Decimal(100))


def test_sell_clamped_to_holdings():
    broker = FakeBroker()
    executor = OrderExecutor(broker, BracketMonitor(broker), TradingGuard(broker))
    executor.execute("t", [Sell("AAPL", Decimal(10))], ctx(holdings={"AAPL": holding("AAPL", 3)}))
    assert broker.created[0].quantity == Decimal(3)
    assert broker.created[0].client_order_id.startswith("t-")


def test_sell_skipped_without_holdings():
    broker = FakeBroker()
    executor = OrderExecutor(broker, BracketMonitor(broker), TradingGuard(broker))
    executor.execute("t", [Sell("AAPL", Decimal(1))], ctx())
    assert broker.created == []


def test_halted_guard_blocks_signals():
    broker = FakeBroker()
    guard = TradingGuard(broker)
    guard.halt("test")
    executor = OrderExecutor(broker, BracketMonitor(broker), guard)
    executor.execute("t", [Buy("AAPL", Decimal(1))], ctx())
    assert broker.created == []


def test_bracket_take_profit_flow():
    broker = FakeBroker()
    brackets = BracketMonitor(broker)
    executor = OrderExecutor(broker, brackets, TradingGuard(broker))

    executor.execute("t", [Buy("AAPL", Decimal(2),
                               take_profit_price=Decimal(310), stop_loss_price=Decimal(280))], ctx())
    assert brackets.active_count == 1

    broker.order_status = OrderStatus.FILLED  # 진입 체결
    signals = brackets.check(ctx(quotes={"AAPL": quote("AAPL", 311)},
                                 holdings={"AAPL": holding("AAPL", 2)}))
    assert len(signals) == 1
    assert signals[0].quantity == Decimal(2)
    assert brackets.active_count == 0


def test_bracket_dropped_when_entry_canceled():
    broker = FakeBroker()
    brackets = BracketMonitor(broker)
    brackets.register("ord_1", "AAPL", Decimal(2), Decimal(310), None)
    broker.order_status = OrderStatus.CANCELED
    assert brackets.check(ctx(quotes={"AAPL": quote("AAPL", 999)})) == []
    assert brackets.active_count == 0


def test_guard_halts_after_consecutive_failures():
    broker = FakeBroker()
    broker.open_orders = [Order(order_id="ord_9", status=OrderStatus.SUBMITTED)]
    guard = TradingGuard(broker, max_consecutive_failures=3)
    for _ in range(3):
        guard.record_failure(RuntimeError("boom"))
    assert guard.is_halted
    assert broker.canceled == ["ord_9"]  # 비상정지 시 미체결 취소
