"""브로커 추상화 인터페이스 + 공용 HTTP/유틸.

엔진/브라켓/가드/PnL 등 모든 컴포넌트는 BrokerClient 에만 의존한다.
새 증권사 지원 = 이 클래스의 서브클래스(어댑터) 추가. 전략 코드는 무수정.
"""
from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from abc import ABC, abstractmethod
from decimal import Decimal
from datetime import timedelta
from zoneinfo import ZoneInfo

from .models import (
    TradingEnvironment,
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
    Fill, Holding, MarketDay, Order, Quote, SessionHours,
)

KST = ZoneInfo("Asia/Seoul")


class BrokerClient(ABC):
    """증권사(브로커) 추상화. 구현 규약:

    - 인증(토큰 갱신 포함)은 어댑터 내부에서 처리한다 - 호출자는 인증을 모른다
    - 실패는 hermetix.errors 의 타입으로 던진다
    - 응답의 방언(부호 접두, zero-padding 등)은 어댑터가 정규화한다
    """

    capabilities: BrokerCapabilities
    # 이 인스턴스가 연결된 거래 환경. 엔진은 LIVE 면 명시 동의(live_trading_enabled)를 요구한다
    environment: TradingEnvironment = TradingEnvironment.PAPER

    @abstractmethod
    def get_quotes(self, symbols: list[str]) -> list[Quote]: ...

    @abstractmethod
    def get_candles(self, symbol: str, interval: CandleInterval, limit: int | None = None) -> list[Candle]: ...

    @abstractmethod
    def get_calendar(self) -> list[MarketDay]: ...

    @abstractmethod
    def get_account(self) -> Account: ...

    @abstractmethod
    def get_holdings(self) -> list[Holding]: ...

    @abstractmethod
    def get_buying_power(self) -> Decimal: ...

    @abstractmethod
    def create_order(self, request: CreateOrderRequest) -> Order: ...

    @abstractmethod
    def get_orders(self) -> list[Order]: ...

    @abstractmethod
    def get_order(self, order_id: str) -> Order: ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> Order: ...

    @abstractmethod
    def get_fills(self) -> list[Fill]: ...


class _Http:
    """stdlib 기반 최소 HTTP 클라이언트 (외부 의존성 0 유지)."""

    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        query: dict[str, str] | None = None,
        json_body: dict | None = None,
        form_body: dict[str, str] | None = None,
    ) -> tuple[int, dict]:
        """(status_code, parsed_json) 을 반환한다. 4xx/5xx 도 본문을 파싱해 반환."""
        url = self.base_url + path
        if query:
            url += "?" + urllib.parse.urlencode(query)

        data = None
        hdrs = dict(headers or {})
        if json_body is not None:
            data = json.dumps(json_body).encode()
            hdrs.setdefault("Content-Type", "application/json; charset=utf-8")
        elif form_body is not None:
            data = urllib.parse.urlencode(form_body).encode()
            hdrs.setdefault("Content-Type", "application/x-www-form-urlencoded")

        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as res:
                return res.status, _parse_json(res.read())
        except urllib.error.HTTPError as e:
            return e.code, _parse_json(e.read())


def _parse_json(raw: bytes) -> dict:
    try:
        parsed = json.loads(raw.decode())
        return parsed if isinstance(parsed, dict) else {"_raw": parsed}
    except (ValueError, UnicodeDecodeError):
        return {}


class Throttle:
    """호출 간 최소 간격 보장 (모의 서버 레이트리밋 회피)."""

    def __init__(self, min_interval_seconds: float):
        self.min_interval = min_interval_seconds
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            delta = self._last + self.min_interval - time.monotonic()
            if delta > 0:
                time.sleep(delta)
            self._last = time.monotonic()


def krx_calendar(days: int = 31) -> list[MarketDay]:
    """KRX 정규장 합성 캘린더 (KIS/키움 공용).

    공휴일은 반영하지 못한다 - 휴장일에는 개장일로 보이지만 주문 시 각 서버가
    거부(장종료)하므로 안전에는 문제가 없다.
    """
    from datetime import datetime
    today = datetime.now(KST).date()
    result = []
    for offset in range(days):
        d = today + timedelta(days=offset)
        weekday = d.weekday() < 5
        result.append(MarketDay(
            date=d.isoformat(),
            open=weekday,
            regular=SessionHours("09:00", "15:30") if weekday else None,
            timezone="Asia/Seoul",
        ))
    return result


def krx_tick_round(price: Decimal) -> Decimal:
    """KRX 호가단위 보정 (2023-01 개정) - 유효 호가로 내림."""
    if price < 2_000: tick = Decimal(1)
    elif price < 5_000: tick = Decimal(5)
    elif price < 20_000: tick = Decimal(10)
    elif price < 50_000: tick = Decimal(50)
    elif price < 200_000: tick = Decimal(100)
    elif price < 500_000: tick = Decimal(500)
    else: tick = Decimal(1_000)
    return (price // tick) * tick
