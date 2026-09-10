"""전략 SPI - 전략 작성자가 구현하는 유일한 표면.

사용 예:

    class MyStrategy(Strategy):
        spec = StrategySpec(name="my-first", symbols=["AAPL"],
                            candle_interval=CandleInterval.DAY_1, candle_limit=20)

        def decide(self, ctx: StrategyContext) -> list[Signal]:
            price = ctx.quote("AAPL")
            if price and not ctx.has_position("AAPL"):
                return [Buy("AAPL", Decimal(1),
                            take_profit_price=price.price * Decimal("1.04"),
                            stop_loss_price=price.price * Decimal("0.98"))]
            return []

규약:
- decide() 는 엔진이 poll_interval 주기로 호출한다 (기본: 해당 시장 정규장 중에만)
- 반환한 Signal 목록은 엔진이 순서대로 실행한다. 할 일 없으면 빈 리스트
- 전략 안에서 브로커 API 를 직접 호출하거나 스레드를 만들지 않는다
- 상태는 인스턴스 속성에 보관한다 (엔진이 인스턴스를 재사용). 재시작 시 사라짐에 유의
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from .models import (
    symbols_match,
    Account, Candle, CandleInterval, Holding, Order, OrderType, Quote, TimeInForce,
)


@dataclass(frozen=True)
class StrategySpec:
    name: str
    symbols: list[str]
    candle_interval: CandleInterval = CandleInterval.DAY_1
    candle_limit: int = 30
    poll_interval_seconds: float = 60.0
    regular_hours_only: bool = True


@dataclass(frozen=True)
class Buy:
    """매수 진입. take_profit/stop_loss 를 지정하면 체결 후 엔진이 자동 청산한다
    (소프트웨어 브라켓 - 앱 메모리 관리, 재시작 시 소실)."""
    symbol: str
    quantity: Decimal
    order_type: OrderType = OrderType.MARKET
    limit_price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.DAY
    take_profit_price: Decimal | None = None
    stop_loss_price: Decimal | None = None


@dataclass(frozen=True)
class Sell:
    """매도 청산. 보유 수량 내로 자동 클램프된다 (공매도 방지)."""
    symbol: str
    quantity: Decimal
    order_type: OrderType = OrderType.MARKET
    limit_price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.DAY


@dataclass(frozen=True)
class Cancel:
    """미체결 주문 취소."""
    order_id: str


Signal = Buy | Sell | Cancel


@dataclass(frozen=True)
class StrategyContext:
    """전략 호출 시점의 시장/계좌 스냅샷. 전략은 이 데이터만으로 판단한다."""
    now: datetime
    quotes: dict[str, Quote]
    candles: dict[str, list[Candle]]
    account: Account
    holdings: dict[str, Holding]
    open_orders: list[Order]
    buying_power: Decimal

    # 심볼 조회는 MARKET:CODE 접두 유무를 무시하고 코드로 맞춘다 (symbols_match)

    def quote(self, symbol: str) -> Quote | None:
        return _by_symbol(self.quotes, symbol)

    def candles_of(self, symbol: str) -> list[Candle]:
        return _by_symbol(self.candles, symbol) or []

    def holding(self, symbol: str) -> Holding | None:
        return _by_symbol(self.holdings, symbol)

    def has_position(self, symbol: str) -> bool:
        h = self.holding(symbol)
        return h is not None and h.quantity > 0

    def open_orders_of(self, symbol: str) -> list[Order]:
        return [o for o in self.open_orders if o.symbol and symbols_match(o.symbol, symbol)]

    def has_open_order(self, symbol: str) -> bool:
        return bool(self.open_orders_of(symbol))


def _by_symbol(mapping: dict, symbol: str):
    if symbol in mapping:
        return mapping[symbol]
    return next((v for k, v in mapping.items() if symbols_match(k, symbol)), None)


class Strategy(ABC):

    spec: StrategySpec

    @abstractmethod
    def decide(self, ctx: StrategyContext) -> list[Signal]: ...
