"""브로커 추상화 인터페이스 + 공용 HTTP/유틸.

엔진/브라켓/가드/PnL 등 모든 컴포넌트는 BrokerClient 에만 의존한다.
새 증권사 지원 = 이 클래스의 서브클래스(어댑터) 추가. 전략 코드는 무수정.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from abc import ABC, abstractmethod
from decimal import Decimal
from datetime import timedelta
from typing import Callable
from zoneinfo import ZoneInfo

from .models import (
    TradingEnvironment,
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
    Fill, Holding, MarketDay, Order, OrderBookTick, OrderEvent, Quote, SessionHours, TradeTick,
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


TradeListener = Callable[[TradeTick], None]
OrderBookListener = Callable[[OrderBookTick], None]
OrderEventListener = Callable[[OrderEvent], None]


class MarketStream(ABC):
    """실시간 시장 데이터 스트림. 규약:

    - connect() 후 연결이 끊기면 스스로 지수 백오프로 재연결하고, 재연결 시 기존 구독을 다시 보낸다
    - subscribe_* 는 연결 전에 불러도 된다 - 연결되는 순간 전송된다
    - 선언하지 않은 채널(BrokerCapabilities.streams)의 subscribe 는 NotImplementedError
    - 리스너는 스트림 스레드에서 호출된다. 오래 걸리는 일은 리스너 안에서 하지 말 것 (엔진은 루프로 넘긴다)
    - 리스너가 던진 예외는 스트림이 삼키고 로그만 남긴다
    - close() 뒤에는 재연결하지 않는다
    """

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """소켓이 열려 있고 (브로커가 요구하면) 로그인까지 끝났는지"""

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def subscribe_trades(self, symbols: list[str], listener: TradeListener) -> None: ...

    def subscribe_order_book(self, symbols: list[str], listener: OrderBookListener) -> None:
        raise NotImplementedError("이 브로커는 호가 스트림을 제공하지 않습니다")

    def subscribe_order_events(self, listener: OrderEventListener) -> None:
        """계좌 전체의 주문 통보 - 심볼 지정 없음"""
        raise NotImplementedError("이 브로커는 주문 통보 스트림을 제공하지 않습니다")

    @abstractmethod
    def close(self) -> None: ...


class StreamingBrokerClient(BrokerClient):
    """실시간 스트림을 제공하는 브로커 어댑터. capabilities.streams 가 비어있지 않은 어댑터만 구현한다.
    엔진은 isinstance(broker, StreamingBrokerClient) 와 capabilities 둘 다 확인한다."""

    @abstractmethod
    def open_stream(self) -> MarketStream:
        """새 스트림 인스턴스를 만든다. 연결은 호출자가 MarketStream.connect() 로 시작한다"""

    def apply_order_event(self, event: OrderEvent) -> None:
        """엔진이 받은 주문 통보를 어댑터에 전달한다. 서버 주문 조회가 없어 메모리로 추적하는 어댑터(KIS 모의)는
        여기서 체결·취소를 반영해 get_order 가 즉시 맞는 상태를 돌려주게 한다. 기본은 아무것도 안 한다"""


class _Http:
    """stdlib 기반 최소 HTTP 클라이언트 (외부 의존성 0 유지)."""

    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        # 직전 응답 헤더 (Retry-After 등). 테스트 스텁은 설정하지 않아도 된다
        self.last_headers: dict[str, str] = {}

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
                self.last_headers = {k.lower(): v for k, v in res.headers.items()}
                return res.status, _parse_json(res.read())
        except urllib.error.HTTPError as e:
            self.last_headers = {k.lower(): v for k, v in (e.headers.items() if e.headers else [])}
            return e.code, _parse_json(e.read())


def _parse_json(raw: bytes) -> dict:
    try:
        parsed = json.loads(raw.decode())
        return parsed if isinstance(parsed, dict) else {"_raw": parsed}
    except (ValueError, UnicodeDecodeError):
        return {}


class Throttle:
    """호출 간 최소 간격 보장 (모의 서버 레이트리밋 회피). 0 이면 쓰로틀 없음."""

    def __init__(self, min_interval_seconds: float, sleep=time.sleep, clock=time.monotonic):
        self.min_interval = min_interval_seconds
        self._sleep = sleep
        self._clock = clock
        self._lock = threading.Lock()
        self._last: float | None = None

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        with self._lock:
            if self._last is not None:
                delta = self._last + self.min_interval - self._clock()
                if delta > 0:
                    self._sleep(delta)
            self._last = self._clock()


class RateLimiter:
    """어댑터 공용 레이트리밋 부품 - 쓰로틀 + RateLimitError 백오프 재시도.

    - min_interval_seconds: 호출 간 최소 간격 (0 = 쓰로틀 없음)
    - max_retries: RateLimitError 재시도 횟수. 소진되면 마지막 예외를 그대로 던진다 - 엔진은 그때서야 틱을 건너뛴다
    - 대기: 서버 Retry-After(retry_after_seconds)가 있으면 그 값(상한 MAX_RETRY_AFTER), 아니면 backoff(attempt)
    """

    MAX_RETRY_AFTER = 30.0

    def __init__(self, min_interval_seconds: float, max_retries: int = 3,
                 backoff=lambda attempt: 1.0 * attempt, sleep=time.sleep, clock=time.monotonic):
        self.throttle = Throttle(min_interval_seconds, sleep=sleep, clock=clock)
        self._max_retries = max_retries
        self._backoff = backoff
        self._sleep = sleep

    @property
    def min_interval(self) -> float:
        return self.throttle.min_interval

    def execute(self, fn, label: str = ""):
        from .errors import RateLimitError  # 순환 import 회피
        attempt = 0
        while True:
            self.throttle.wait()
            try:
                return fn()
            except RateLimitError as e:
                if attempt >= self._max_retries:
                    raise
                attempt += 1
                wait = (min(e.retry_after_seconds, self.MAX_RETRY_AFTER) if e.retry_after_seconds is not None
                        else self._backoff(attempt))
                logging.getLogger("hermetix").warning("rate limit%s - retry %d/%d after %.1fs",
                                                      f"({label})" if label else "", attempt, self._max_retries, wait)
                self._sleep(wait)


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
