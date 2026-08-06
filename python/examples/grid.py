"""목표가 스캘핑 전략 (다중 종목, 캔들 미사용 - KRX 브로커 호환).

딥 지정가 매수(추격 포함) -> 평균단가 x (1 + target x count) 지정가 매도(GTC)
-> 미체결 감쇠(count--) -> count 소진 후 수익권이면 시장가 청산, 아니면 본전 대기.

실행:  HERMETIX_BROKER=kis KIS_APPKEY=... python examples/grid.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_EVEN

sys.path.insert(0, __file__.rsplit("/", 2)[0])

from hermetix import (
    Buy, Cancel, CandleInterval, OrderSide, OrderType, Sell, Signal,
    Strategy, StrategyContext, StrategySpec, TimeInForce,
)


class GridStrategy(Strategy):

    def __init__(self, symbols: list[str] | None = None,
                 buy_dip_rate: Decimal = Decimal("0.003"), target_rate: Decimal = Decimal("0.01"),
                 max_decay_count: int = 3, decay_minutes: int = 30,
                 chase_rate: Decimal = Decimal("0.005"), budget_ratio: Decimal = Decimal("0.5"),
                 poll_seconds: float = 30.0):
        self.symbols = symbols or ["AAPL"]
        self.buy_dip_rate = buy_dip_rate
        self.target_rate = target_rate
        self.max_decay_count = max_decay_count
        self.decay_minutes = decay_minutes
        self.chase_rate = chase_rate
        self.budget_ratio = budget_ratio
        self.spec = StrategySpec(
            name="grid", symbols=self.symbols,
            candle_interval=CandleInterval.DAY_1, candle_limit=2,  # 캔들 미사용
            poll_interval_seconds=poll_seconds,
        )
        self._decay_count: dict[str, int] = {}
        self._sell_placed_at: dict[str, datetime] = {}

    def decide(self, ctx: StrategyContext) -> list[Signal]:
        signals: list[Signal] = []
        for symbol in self.symbols:
            signals.extend(self._decide_symbol(symbol, ctx))
        return signals

    def _decide_symbol(self, symbol: str, ctx: StrategyContext) -> list[Signal]:
        quote = ctx.quote(symbol)
        if quote is None:
            return []
        if ctx.has_position(symbol):
            return self._decide_sell(symbol, quote.price, ctx)
        self._decay_count[symbol] = self.max_decay_count
        self._sell_placed_at.pop(symbol, None)
        return self._decide_buy(symbol, quote.price, ctx)

    def _decide_buy(self, symbol: str, price: Decimal, ctx: StrategyContext) -> list[Signal]:
        desired = (price * (1 - self.buy_dip_rate)).quantize(Decimal("0.01"), ROUND_HALF_EVEN)
        open_buys = [o for o in ctx.open_orders_of(symbol) if o.side == OrderSide.BUY]
        if open_buys:
            order = open_buys[0]
            if order.limit_price is not None and desired > order.limit_price * (1 + self.chase_rate):
                return [Cancel(order_id=order.order_id)]  # 가격 이탈 - 추격 취소 후 다음 틱 재주문
            return []

        budget = ctx.buying_power * self.budget_ratio / len(self.symbols)
        quantity = (budget / desired).quantize(Decimal(1), rounding=ROUND_DOWN)
        if quantity < 1:
            return []
        return [Buy(symbol=symbol, quantity=quantity, order_type=OrderType.LIMIT,
                    limit_price=desired, time_in_force=TimeInForce.DAY)]

    def _decide_sell(self, symbol: str, price: Decimal, ctx: StrategyContext) -> list[Signal]:
        holding = ctx.holding(symbol)
        if holding is None:
            return []
        entry = holding.avg_entry_price
        count = self._decay_count.setdefault(symbol, self.max_decay_count)

        open_sells = [o for o in ctx.open_orders_of(symbol) if o.side == OrderSide.SELL]
        if open_sells:
            placed_at = self._sell_placed_at.setdefault(symbol, ctx.now)
            if count > 0 and ctx.now - placed_at >= timedelta(minutes=self.decay_minutes):
                self._decay_count[symbol] = count - 1     # 목표 감쇠
                self._sell_placed_at.pop(symbol, None)
                return [Cancel(order_id=open_sells[0].order_id)]
            return []

        if count <= 0 and price >= entry:                 # 수익권 - 즉시 시장가 청산
            return [Sell(symbol=symbol, quantity=holding.quantity)]

        multiplier = 1 + self.target_rate * max(count, 0)
        sell_price = (entry * multiplier).quantize(Decimal("0.01"), ROUND_HALF_EVEN)
        self._sell_placed_at[symbol] = ctx.now
        return [Sell(symbol=symbol, quantity=holding.quantity, order_type=OrderType.LIMIT,
                     limit_price=sell_price, time_in_force=TimeInForce.GTC)]


if __name__ == "__main__":
    from _runner import run
    run(GridStrategy(symbols=["005930", "000660"]))
