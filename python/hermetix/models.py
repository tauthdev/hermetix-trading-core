"""공통 도메인 모델 (Kotlin hermetix-broker 의 dto 와 동일 의미).

금액/수량은 전부 Decimal 이다 - float 를 절대 섞지 말 것.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum


class CandleInterval(Enum):
    MIN_1 = "1m"
    MIN_5 = "5m"
    HOUR_1 = "1h"
    DAY_1 = "1d"


class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class TimeInForce(Enum):
    DAY = "DAY"
    GTC = "GTC"


class OrderStatus(Enum):
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"

    @property
    def is_open(self) -> bool:
        return self in (OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED)


@dataclass(frozen=True)
class Quote:
    symbol: str
    price: Decimal
    bid_price: Decimal | None
    ask_price: Decimal | None
    volume: int
    change: Decimal | None
    change_rate: Decimal | None
    timestamp: datetime


@dataclass(frozen=True)
class Candle:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int


@dataclass(frozen=True)
class SessionHours:
    start: str  # "09:30"
    end: str    # "16:00"


@dataclass(frozen=True)
class MarketDay:
    date: str  # "2026-08-05"
    open: bool
    regular: SessionHours | None
    timezone: str
    holiday: str | None = None


@dataclass(frozen=True)
class Account:
    account_id: str
    currency: str
    cash: Decimal
    portfolio_value: Decimal
    status: str = "ACTIVE"
    name: str | None = None


@dataclass(frozen=True)
class Holding:
    symbol: str
    quantity: Decimal
    avg_entry_price: Decimal
    current_price: Decimal | None = None
    market_value: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    unrealized_pnl_rate: Decimal | None = None


@dataclass(frozen=True)
class CreateOrderRequest:
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    limit_price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.DAY
    client_order_id: str | None = None


@dataclass(frozen=True)
class Order:
    order_id: str
    status: OrderStatus
    symbol: str | None = None
    side: OrderSide | None = None
    order_type: OrderType | None = None
    quantity: Decimal | None = None
    limit_price: Decimal | None = None
    filled_quantity: Decimal | None = None
    avg_fill_price: Decimal | None = None
    client_order_id: str | None = None
    submitted_at: datetime | None = None
    canceled_at: datetime | None = None


@dataclass(frozen=True)
class Fill:
    fill_id: str | None
    order_id: str | None
    symbol: str | None
    side: OrderSide | None
    quantity: Decimal | None
    price: Decimal | None


@dataclass(frozen=True)
class BrokerCapabilities:
    """브로커가 지원하는 기능의 코드 선언. 실측으로 확인한 것만 True 로 선언한다."""
    broker_id: str
    market: str          # "US" | "KRX"
    currency: str
    candle_intervals: frozenset[CandleInterval]
    client_order_id: bool
    native_bracket: bool
    fractional_shares: bool
    server_open_orders: bool = True
