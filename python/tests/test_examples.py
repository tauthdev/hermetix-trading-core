"""예제 전략 3종 핵심 시나리오 - Kotlin 전략 레포의 테스트와 동일 정답지."""
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "examples"))

from grid import GridStrategy
from larry import LarryStrategy
from trend_breakout import TrendBreakoutStrategy, TrendLine

from hermetix import (
    Account, Buy, Candle, Cancel, Holding, Order, OrderSide, OrderStatus,
    OrderType, Quote, Sell, StrategyContext,
)


def candle(ts_offset_h: int, open_: str, close: str, high: str | None = None, low: str | None = None,
           volume: int = 1000) -> Candle:
    base = datetime(2026, 8, 6, tzinfo=timezone.utc)
    o, c = Decimal(open_), Decimal(close)
    return Candle(timestamp=base + timedelta(hours=ts_offset_h), open=o,
                  high=Decimal(high) if high else max(o, c), low=Decimal(low) if low else min(o, c),
                  close=c, volume=volume)


def ctx(candles=None, price="100", holding_qty=None, avg_entry="100", open_orders=None, now=None):
    symbol = "AAPL"
    return StrategyContext(
        now=now or datetime.now(timezone.utc),
        quotes={symbol: Quote(symbol, Decimal(price), None, None, 0, None, None, datetime.now(timezone.utc))},
        candles={symbol: candles or []},
        account=Account("acc", "USD", Decimal(10000), Decimal(10000)),
        holdings={symbol: Holding(symbol, Decimal(holding_qty), Decimal(avg_entry))} if holding_qty else {},
        open_orders=open_orders or [],
        buying_power=Decimal(10000),
    )


# ------------------------------------------------------------------- larry

def larry_candles(target_open: str, target_close: str, lookback: int = 5):
    flat = [candle(i, "100", "101") for i in range(lookback)]
    target = candle(lookback, target_open, target_close)
    in_progress = candle(lookback + 1, target_close, target_close)
    return flat + [target, in_progress]


def test_larry_entry_with_stop_at_open():
    s = LarryStrategy(symbols=["AAPL"], lookback=5)
    signals = s.decide(ctx(candles=larry_candles("100", "108"), price="108"))
    assert len(signals) == 1 and isinstance(signals[0], Buy)
    assert signals[0].stop_loss_price == Decimal("100")
    assert signals[0].quantity == Decimal(46)  # 10000*0.5/108


def test_larry_skips_bearish():
    s = LarryStrategy(symbols=["AAPL"], lookback=5)
    assert s.decide(ctx(candles=larry_candles("108", "100"), price="100")) == []


def test_larry_no_duplicate_entry():
    s = LarryStrategy(symbols=["AAPL"], lookback=5)
    data = larry_candles("100", "108")
    assert len(s.decide(ctx(candles=data, price="108"))) == 1
    assert s.decide(ctx(candles=data, price="108")) == []


def test_larry_expire_exit():
    s = LarryStrategy(symbols=["AAPL"], lookback=5, expire_hours=48)
    now = datetime.now(timezone.utc)
    s._entry_at["AAPL"] = now - timedelta(hours=49)
    signals = s.decide(ctx(candles=larry_candles("100", "101"), holding_qty="46", now=now))
    assert len(signals) == 1 and isinstance(signals[0], Sell)


# ---------------------------------------------------------- trend breakout

def trend_candles(count: int, start_high: float, step: float):
    result = []
    for i in range(count + 1):
        high = start_high + step * i
        result.append(candle(i, str(high - 2), str(high - 1), high=str(high), low=str(high - 4)))
    return result


def test_trend_entry_on_breakout():
    s = TrendBreakoutStrategy(symbols=["AAPL"], lookback=10)
    # 고점 100→110 기울기 +1: 예측고점 111... 예측저점 107, 갭 4 → 돌파선 예측고점+갭
    data = trend_candles(10, 100.0, 1.0)
    trend = TrendLine.of(data[:-1][-10:])
    price = trend.breakout_line + 1
    signals = s.decide(ctx(candles=data, price=str(price)))
    assert len(signals) == 1 and isinstance(signals[0], Buy)
    assert signals[0].take_profit_price is not None and signals[0].stop_loss_price is not None


def test_trend_skips_downtrend():
    s = TrendBreakoutStrategy(symbols=["AAPL"], lookback=10)
    assert s.decide(ctx(candles=trend_candles(10, 110.0, -1.0), price="999")) == []


def test_trend_no_reentry_same_candle():
    s = TrendBreakoutStrategy(symbols=["AAPL"], lookback=10)
    data = trend_candles(10, 100.0, 1.0)
    price = str(TrendLine.of(data[:-1][-10:]).breakout_line + 1)
    assert len(s.decide(ctx(candles=data, price=price))) == 1
    assert s.decide(ctx(candles=data, price=price)) == []


# -------------------------------------------------------------------- grid

def open_order(side: OrderSide, limit: str) -> Order:
    return Order(order_id="ord_1", status=OrderStatus.SUBMITTED, symbol="AAPL",
                 side=side, order_type=OrderType.LIMIT, limit_price=Decimal(limit))


def test_grid_places_dip_buy():
    s = GridStrategy(symbols=["AAPL"])
    signals = s.decide(ctx(price="100"))
    assert isinstance(signals[0], Buy)
    assert signals[0].limit_price == Decimal("99.70")


def test_grid_chases_runaway_price():
    s = GridStrategy(symbols=["AAPL"])
    signals = s.decide(ctx(price="102", open_orders=[open_order(OrderSide.BUY, "99.70")]))
    assert isinstance(signals[0], Cancel)


def test_grid_sell_target_with_count():
    s = GridStrategy(symbols=["AAPL"])
    signals = s.decide(ctx(price="100", holding_qty="50", avg_entry="100"))
    assert isinstance(signals[0], Sell)
    assert signals[0].limit_price == Decimal("103.00")  # 1 + 0.01*3


def test_grid_decay_then_market_exit_when_profitable():
    s = GridStrategy(symbols=["AAPL"], decay_minutes=30)
    now = datetime.now(timezone.utc)
    s._decay_count["AAPL"] = 1
    s._sell_placed_at["AAPL"] = now - timedelta(minutes=31)
    signals = s.decide(ctx(price="100", holding_qty="50",
                           open_orders=[open_order(OrderSide.SELL, "101.00")], now=now))
    assert isinstance(signals[0], Cancel)               # 감쇠: 취소 + count 0
    assert s._decay_count["AAPL"] == 0

    signals = s.decide(ctx(price="100.5", holding_qty="50", avg_entry="100", now=now))
    assert isinstance(signals[0], Sell)
    assert signals[0].order_type == OrderType.MARKET    # 수익권 - 시장가 청산
