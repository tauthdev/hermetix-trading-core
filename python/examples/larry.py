"""Larry Williams 식 변동성 돌파 전략 (롱 온리, 다중 종목).

직전 완성 캔들의 몸통이 평균 몸통 x multiplier 이상인 양봉이면 시장가 매수,
손절은 진입 캔들 시가(브라켓 위임), expire_hours 경과 시 만료 청산.

실행:  HERMETIX_BROKER=next NEXT_CLIENT_ID=... python examples/larry.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN

sys.path.insert(0, __file__.rsplit("/", 2)[0])  # python/ 루트

from hermetix import Buy, CandleInterval, Sell, Signal, Strategy, StrategyContext, StrategySpec


class LarryStrategy(Strategy):

    def __init__(self, symbols: list[str] | None = None, lookback: int = 24,
                 multiplier: Decimal = Decimal("1.2"), expire_hours: int = 48,
                 budget_ratio: Decimal = Decimal("0.5"),
                 candle_interval: CandleInterval = CandleInterval.HOUR_1):
        self.symbols = symbols or ["AAPL"]
        self.lookback = lookback
        self.multiplier = multiplier
        self.expire_hours = expire_hours
        self.budget_ratio = budget_ratio
        self.spec = StrategySpec(
            name="larry", symbols=self.symbols,
            candle_interval=candle_interval, candle_limit=lookback + 2,
        )
        self._last_evaluated: dict[str, datetime] = {}
        self._entry_at: dict[str, datetime] = {}

    def decide(self, ctx: StrategyContext) -> list[Signal]:
        signals: list[Signal] = []
        for symbol in self.symbols:
            signals.extend(self._decide_symbol(symbol, ctx))
        return signals

    def _decide_symbol(self, symbol: str, ctx: StrategyContext) -> list[Signal]:
        if ctx.has_position(symbol):
            return self._decide_exit(symbol, ctx)
        self._entry_at.pop(symbol, None)
        if ctx.has_open_order(symbol):
            return []
        return self._decide_entry(symbol, ctx)

    def _decide_entry(self, symbol: str, ctx: StrategyContext) -> list[Signal]:
        candles = ctx.candles_of(symbol)
        if len(candles) < self.lookback + 2:
            return []
        completed = candles[:-1]           # 마지막 캔들은 진행 중
        target = completed[-1]
        if self._last_evaluated.get(symbol) == target.timestamp:
            return []                       # 같은 캔들로 중복 진입 방지
        self._last_evaluated[symbol] = target.timestamp

        history = completed[-(self.lookback + 1):-1]
        avg_body = sum(abs(c.open - c.close) for c in history) / len(history)
        if abs(target.open - target.close) < avg_body * self.multiplier:
            return []
        if target.open >= target.close:     # 롱 온리 - 음봉 돌파 스킵
            return []

        quote = ctx.quote(symbol)
        if quote is None:
            return []
        budget = ctx.buying_power * self.budget_ratio / len(self.symbols)
        quantity = (budget / quote.price).quantize(Decimal(1), rounding=ROUND_DOWN)
        if quantity < 1:
            return []

        self._entry_at[symbol] = ctx.now
        return [Buy(symbol=symbol, quantity=quantity, stop_loss_price=target.open)]

    def _decide_exit(self, symbol: str, ctx: StrategyContext) -> list[Signal]:
        opened_at = self._entry_at.setdefault(symbol, ctx.now)  # 재시작 시 만료 클록 재시작
        if ctx.now - opened_at < timedelta(hours=self.expire_hours):
            return []
        holding = ctx.holding(symbol)
        if holding is None:
            return []
        self._entry_at.pop(symbol, None)
        return [Sell(symbol=symbol, quantity=holding.quantity)]


if __name__ == "__main__":
    from _runner import run  # noqa: E402
    run(LarryStrategy())
