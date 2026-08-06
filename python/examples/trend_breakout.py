"""WMA 추세선 돌파 전략 (롱 온리, 다중 종목).

가중 이동 추세선으로 예측 고점/저점을 구하고, 현재가가 돌파선(예측고점+갭)을
넘고 상승 추세면 시장가 매수. 익절 +profit_rate, 손절 지지선(브라켓 위임).

실행:  HERMETIX_BROKER=next NEXT_CLIENT_ID=... python examples/trend_breakout.py
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_EVEN

sys.path.insert(0, __file__.rsplit("/", 2)[0])

from hermetix import Buy, Candle, CandleInterval, Sell, Signal, Strategy, StrategyContext, StrategySpec


@dataclass(frozen=True)
class TrendLine:
    high_price: Decimal
    low_price: Decimal
    high_inclination: Decimal
    low_inclination: Decimal
    avg_volume: Decimal

    @property
    def gap(self) -> Decimal:
        return self.high_price - self.low_price

    @property
    def breakout_line(self) -> Decimal:
        return self.high_price + self.gap

    @property
    def support_line(self) -> Decimal:
        return self.low_price - self.gap

    @staticmethod
    def of(candles: list[Candle]) -> "TrendLine":
        weight = Decimal(0)
        volume_weight = Decimal(1)
        high_inc = Decimal(0)
        low_inc = Decimal(0)
        avg_volume = Decimal(candles[0].volume)
        for i in range(1, len(candles)):
            w = Decimal(i)
            weight += w
            volume_weight += Decimal(i + 1)
            high_inc += (candles[i].high - candles[i - 1].high) * w
            low_inc += (candles[i].low - candles[i - 1].low) * w
            avg_volume += Decimal(candles[i].volume) * Decimal(i + 1)
        high_inc = (high_inc / weight).quantize(Decimal("0.00000001"), ROUND_HALF_EVEN)
        low_inc = (low_inc / weight).quantize(Decimal("0.00000001"), ROUND_HALF_EVEN)
        return TrendLine(
            high_price=candles[-1].high + high_inc,
            low_price=candles[-1].low + low_inc,
            high_inclination=high_inc,
            low_inclination=low_inc,
            avg_volume=(avg_volume / volume_weight).quantize(Decimal("0.00000001"), ROUND_HALF_EVEN),
        )


class TrendBreakoutStrategy(Strategy):

    def __init__(self, symbols: list[str] | None = None, lookback: int = 120,
                 profit_rate: Decimal = Decimal("0.04"), volume_ratio: Decimal = Decimal(0),
                 expire_hours: int = 12, budget_ratio: Decimal = Decimal("0.5"),
                 candle_interval: CandleInterval = CandleInterval.HOUR_1):
        self.symbols = symbols or ["AAPL"]
        self.lookback = lookback
        self.profit_rate = profit_rate
        self.volume_ratio = volume_ratio
        self.expire_hours = expire_hours
        self.budget_ratio = budget_ratio
        self.spec = StrategySpec(
            name="trend-breakout", symbols=self.symbols,
            candle_interval=candle_interval, candle_limit=lookback + 1,
        )
        self._last_entry_candle: dict[str, datetime] = {}
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
        if len(candles) < self.lookback + 1:
            return []
        completed, in_progress = candles[:-1], candles[-1]
        if self._last_entry_candle.get(symbol) == in_progress.timestamp:
            return []                       # 같은 시간봉 구간 재진입 방지

        trend = TrendLine.of(completed[-self.lookback:])
        if trend.high_inclination <= 0:     # 상승 추세만
            return []
        quote = ctx.quote(symbol)
        if quote is None or quote.price < trend.breakout_line:
            return []
        if self.volume_ratio > 0 and Decimal(in_progress.volume) < trend.avg_volume * self.volume_ratio:
            return []

        budget = ctx.buying_power * self.budget_ratio / len(self.symbols)
        quantity = (budget / quote.price).quantize(Decimal(1), rounding=ROUND_DOWN)
        if quantity < 1:
            return []

        self._last_entry_candle[symbol] = in_progress.timestamp
        self._entry_at[symbol] = ctx.now
        return [Buy(
            symbol=symbol, quantity=quantity,
            take_profit_price=(quote.price * (1 + self.profit_rate)).quantize(Decimal("0.01"), ROUND_HALF_EVEN),
            stop_loss_price=trend.support_line.quantize(Decimal("0.01"), ROUND_HALF_EVEN),
        )]

    def _decide_exit(self, symbol: str, ctx: StrategyContext) -> list[Signal]:
        opened_at = self._entry_at.setdefault(symbol, ctx.now)
        if ctx.now - opened_at < timedelta(hours=self.expire_hours):
            return []
        holding = ctx.holding(symbol)
        if holding is None:
            return []
        self._entry_at.pop(symbol, None)
        return [Sell(symbol=symbol, quantity=holding.quantity)]


if __name__ == "__main__":
    from _runner import run
    run(TrendBreakoutStrategy())
