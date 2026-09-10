"""공통 도메인 모델 (Kotlin hermetix-broker 의 dto 와 동일 의미).

금액/수량은 전부 Decimal 이다 - float 를 절대 섞지 말 것.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
import re
from enum import Enum


class TradingEnvironment(Enum):
    """거래 환경. PAPER=모의투자(기본), LIVE=실전투자 — 엔진은 live_trading_enabled=True 없이는 LIVE 를 기동하지 않는다.
    키는 항상 사용자 기기에서만 쓰인다."""
    PAPER = "PAPER"
    LIVE = "LIVE"


_MARKET_PREFIX = re.compile(r"^([A-Z]{2,6}):(.+)$")


def parse_symbol(symbol: str) -> tuple[str | None, str]:
    """`MARKET:CODE` 표기를 (market, code) 로 나눈다. 접두가 없으면 (None, symbol)."""
    m = _MARKET_PREFIX.match(symbol)
    return (m.group(1), m.group(2)) if m else (None, symbol)


def symbol_code(symbol: str) -> str:
    """접두를 뗀 브로커 심볼 코드"""
    return parse_symbol(symbol)[1]


def symbols_match(a: str, b: str) -> bool:
    """코드가 같고, 둘 다 시장을 명시했다면 시장도 같아야 한다"""
    ma, ca = parse_symbol(a)
    mb, cb = parse_symbol(b)
    return ca == cb and (ma is None or mb is None or ma == mb)


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
    """주문 상태 (넥스트증권 공개 스펙 v1.3 부록 D 7종 + UNKNOWN 폴백).

    PENDING_CANCEL 은 취소 접수 후 미확정 상태 — 원주문이 체결될 수 있으므로 OPEN 으로 분류한다.
    """
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    PENDING_CANCEL = "PENDING_CANCEL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"

    @property
    def is_open(self) -> bool:
        return self in (OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING_CANCEL)


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
    market: str          # 기본 시장 "US" | "KRX" — 접두 없는 심볼은 이 시장으로 해석
    currency: str
    candle_intervals: frozenset[CandleInterval]
    client_order_id: bool
    native_bracket: bool
    fractional_shares: bool
    server_open_orders: bool = True
    # 지원 거래 환경. 실전(LIVE)은 실측으로 확인한 어댑터만 선언
    environments: frozenset[TradingEnvironment] = frozenset({TradingEnvironment.PAPER})
    # 한 계좌로 다룰 수 있는 시장 목록 (MARKET:CODE 접두 허용 값). None 이면 {market}
    markets: frozenset[str] | None = None

    def __post_init__(self):
        if self.markets is None:
            object.__setattr__(self, "markets", frozenset({self.market}))

    def symbol_code(self, symbol: str) -> str:
        """심볼의 시장 접두가 지원 시장인지 확인하고 브로커 코드를 돌려준다. 미지원이면 ValueError."""
        market, code = parse_symbol(symbol)
        if market is not None and market not in self.markets:
            raise ValueError(f"브로커 '{self.broker_id}' 는 시장 '{market}' 을 지원하지 않습니다 (지원: {sorted(self.markets)}): {symbol}")
        return code
