"""토스증권 실시간 스트림. 공식 AsyncAPI 1.2.2 문서 기반, **실측 전** (모의투자 서버가 없어 실계좌로만 검증 가능). Kotlin TossMarketStream 과 동일 의미.

프로토콜 (wss://openapi-ws.tossinvest.com/ws/v1, openapi.tossinvest.com/openapi-docs/latest/asyncapi.json):
- 핸드셰이크에 Authorization: Bearer {access_token} - REST 와 같은 토큰. 토큰은 접속 때만 검사된다
- 구독은 선언형: 클라이언트가 보내는 JSON 배열 하나가 현재 구독 집합 전체다. 새 배열이 이전 집합을 통째로 대체하고, 빠진 항목은
  자동 해제된다. 요소는 {"type":"trade:kr","codes":["005930"]} 꼴, 첫 요소로 {"id":"req-N"} 을 넣으면 응답에 echo 된다.
  type: trade:us / trade:kr / orderbook:us / orderbook:kr (codes = 종목코드, 미국은 대문자 티커), personal:order (codes = accountSeq 문자열)
- 서버 프레임은 type 으로 구분: subscriptions(구독 결과, subscribed[]·rejected[]{target,code,message}), message(데이터,
  topic = trade:kr:005930 처럼 {type}:{code}), error(선언 전체 실패·서버 재시작 server-shutdown), pong
- 180초 동안 클라이언트가 아무것도 보내지 않으면 서버가 끊는다 -> 60초마다 텍스트 프레임 PING (JSON 아님) -> {"type":"pong"}
- 한도: 계정당 연결 2개, 연결당 구독 100개, 선언 5회/초 -> 구독 변경은 declare_delay_seconds 동안 모아 한 번에 보낸다
- trade/orderbook 은 유실 가능(백프레셔 시 최신 우선), personal:order 는 세션 내 무손실. 재접속 뒤 놓친 이벤트는 재전송되지 않는다

프레임 (모든 숫자는 문자열):
- trade data: price, volume(이 체결 수량), timestamp(ISO-8601 +09:00), currency. 누적거래량·등락·매수매도 구분 없음
- orderbook data: timestamp(null 가능), currency, asks[]/bids[] {price, volume} (최우선부터, 단계 수 미명시)
- personal:order data: event (PENDING / PARTIAL_FILL / FILL / CANCELING / CANCELED / REPLACING / REPLACED / REJECTED / CANCEL_REJECTED /
  REPLACE_REJECTED), accountSeq, order (REST 주문 상세와 같은 스냅샷 - execution.filledQuantity 는 누적이라 이번 체결량은 직전 스냅샷과의 차이)

문서로 확정하지 못한 점(실측 필요): 호가 단계 수, 취소·정정 시 어떤 orderId 로 이벤트가 오는지(취소는 REST 에서 새 orderId 를 발급한다), 표준 ping 프레임이 유휴 타이머를 리셋하는지.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable

from ..broker import MarketStream, OrderBookListener, OrderEventListener, TradeListener
from ..models import OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType, OrderSide, TradeTick, parse_symbol
from ..models import StreamChannel
from ..stream import ReconnectingWebSocket

logger = logging.getLogger("hermetix")

_EVENT_TYPES = {
    "PENDING": OrderEventType.ACCEPTED,
    "PARTIAL_FILL": OrderEventType.FILLED,
    "FILL": OrderEventType.FILLED,
    "CANCELED": OrderEventType.CANCELED,
    "REPLACED": OrderEventType.MODIFIED,
    "REJECTED": OrderEventType.REJECTED,
    "CANCEL_REJECTED": OrderEventType.REJECTED,
    "REPLACE_REJECTED": OrderEventType.REJECTED,
}


def topic_key(symbol: str) -> str:
    """Hermetix 심볼 -> topic 키 (KRX:005930/005930 -> kr:005930, US:AAPL -> us:AAPL)"""
    market, code = parse_symbol(symbol)
    if market in (None, "KRX"):
        return f"kr:{code}"
    if market == "US":
        return f"us:{code.upper()}"
    raise ValueError(f"토스 어댑터가 지원하지 않는 시장: {market} ({symbol})")


def canonical_symbol(market: str, code: str) -> str:
    """topic 의 시장·코드 -> 정규 표기 (KRX:005930, US:AAPL)"""
    return f"US:{code}" if market == "us" else f"KRX:{code}"


def _dec(node: dict, field: str) -> Decimal | None:
    value = node.get(field)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def _ts(node: dict, field: str) -> datetime | None:
    value = node.get(field)
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_toss_trade(topic: str, data: dict) -> TradeTick | None:
    """trade:{kr|us}:{code} 데이터 -> 체결. 누적거래량·등락·호가는 프레임에 없어 None"""
    parts = topic.split(":", 2)
    if len(parts) < 3 or parts[0] != "trade":
        return None
    price = _dec(data, "price")
    if price is None:
        return None
    return TradeTick(
        symbol=canonical_symbol(parts[1], parts[2]),
        price=price,
        quantity=_dec(data, "volume") or Decimal(0),
        timestamp=_ts(data, "timestamp") or datetime.now(timezone.utc),
    )


def parse_toss_order_book(topic: str, data: dict) -> OrderBookTick | None:
    """orderbook:{kr|us}:{code} 데이터 -> 호가창. asks 오름차순·bids 내림차순으로 오므로 순서 그대로가 최우선부터"""
    parts = topic.split(":", 2)
    if len(parts) < 3 or parts[0] != "orderbook":
        return None

    def levels(items) -> list[OrderBookLevel]:
        out = []
        for level in items or []:
            price = _dec(level, "price")
            if price is None:
                continue
            out.append(OrderBookLevel(price, _dec(level, "volume") or Decimal(0)))
        return out
    return OrderBookTick(
        symbol=canonical_symbol(parts[1], parts[2]),
        timestamp=_ts(data, "timestamp") or datetime.now(timezone.utc),
        asks=levels(data.get("asks")),
        bids=levels(data.get("bids")),
    )


def parse_toss_order_event(data: dict, previous_filled: Decimal | None) -> OrderEvent | None:
    """personal:order 데이터 -> 주문 이벤트. previous_filled 는 같은 orderId 의 직전 누적 체결량 (없으면 이번이 첫 스냅샷).
    CANCELING/REPLACING 은 중간 상태라 None."""
    order = data.get("order") or {}
    order_id = str(order.get("orderId") or "").strip()
    if not order_id:
        return None
    execution = order.get("execution") or {}
    filled = _dec(execution, "filledQuantity") or Decimal(0)
    quantity = _dec(order, "quantity")
    price = _dec(order, "price")
    event_name = str(data.get("event") or "")
    kind = _EVENT_TYPES.get(event_name)
    if kind is None:
        return None  # CANCELING, REPLACING, 미지 값
    market = "US" if order.get("currency") == "USD" else "KRX"
    raw_symbol = str(order.get("symbol") or "").strip()
    fill_quantity = filled - (previous_filled or Decimal(0))
    if fill_quantity < 0:
        fill_quantity = filled
    return OrderEvent(
        order_id=order_id,
        type=kind,
        timestamp=_ts(order, "orderedAt") or datetime.now(timezone.utc),
        symbol=f"{market}:{raw_symbol}" if raw_symbol else None,
        side={"BUY": OrderSide.BUY, "SELL": OrderSide.SELL}.get(str(order.get("side") or "")),
        quantity=fill_quantity if kind == OrderEventType.FILLED else quantity,
        price=(_dec(execution, "averageFilledPrice") or price) if kind == OrderEventType.FILLED else price,
        remaining_quantity=(quantity - filled) if quantity is not None else None,
        reason=event_name if kind == OrderEventType.REJECTED else None,
    )


class TossMarketStream(ReconnectingWebSocket, MarketStream):

    def __init__(self, ws_url: str, token: Callable[[], str], account_seq: Callable[[], str],
                 heartbeat_seconds: float = 60.0, declare_delay_seconds: float = 0.2, usage=None):
        super().__init__("toss", idle_timeout_seconds=0, heartbeat_seconds=heartbeat_seconds, usage=usage)
        self._ws_url = ws_url
        self._token = token
        self._account_seq = account_seq
        self._declare_delay = declare_delay_seconds
        self._lock = threading.Lock()
        self._trade_listeners: dict[str, list[TradeListener]] = {}     # topic 키(kr:005930) -> 리스너
        self._book_listeners: dict[str, list[OrderBookListener]] = {}
        self._order_listeners: list[OrderEventListener] = []
        self._requested: dict[str, str] = {}                            # topic 키 -> 구독 요청 표기
        self._rejected: set[str] = set()                                # 서버가 거부한 target - 다시 선언하지 않는다
        self._filled_so_far: dict[str, Decimal] = {}                    # orderId -> 직전 스냅샷의 누적 체결량
        self._pending_declare: threading.Timer | None = None
        self._request_counter = 0

    @property
    def is_connected(self) -> bool:
        return self.is_socket_open

    def uri(self) -> str:
        return self._ws_url

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token()}"}

    def on_open(self) -> None:
        self.declare_now()

    def on_heartbeat(self) -> None:
        self.send("PING")

    def close(self) -> None:
        with self._lock:
            timer, self._pending_declare = self._pending_declare, None
        if timer is not None:
            timer.cancel()
        super().close()

    # ------------------------------------------------------------------ subscribe

    def _register(self, symbols: list[str], listener, target: dict) -> int:
        """새로 생긴 topic 수 (0 이 아니면 선언을 다시 보내야 한다)"""
        changed = 0
        with self._lock:
            for symbol in symbols:
                key = topic_key(symbol)
                self._requested.setdefault(key, symbol)
                if key not in target:
                    target[key] = []
                    changed += 1
                target[key].append(listener)
        return changed

    def subscribe_trades(self, symbols: list[str], listener: TradeListener) -> None:
        new = self._register(symbols, listener, self._trade_listeners)
        if new:
            if self._usage is not None:
                self._usage.stream_subscribed(StreamChannel.TRADES, new)
            self._schedule_declare()

    def subscribe_order_book(self, symbols: list[str], listener: OrderBookListener) -> None:
        new = self._register(symbols, listener, self._book_listeners)
        if new:
            if self._usage is not None:
                self._usage.stream_subscribed(StreamChannel.ORDER_BOOK, new)
            self._schedule_declare()

    def subscribe_order_events(self, listener: OrderEventListener) -> None:
        with self._lock:
            first = not self._order_listeners
            self._order_listeners.append(listener)
        if first and self._usage is not None:
            self._usage.stream_subscribed(StreamChannel.ORDER_EVENTS)
        if first:
            self._schedule_declare()

    def _schedule_declare(self) -> None:
        if not self.is_socket_open:
            return  # 접속되면 on_open 이 전체를 선언한다
        with self._lock:
            if self._pending_declare is not None:
                self._pending_declare.cancel()
            timer = threading.Timer(self._declare_delay, self.declare_now)
            timer.daemon = True
            self._pending_declare = timer
        timer.start()

    def declare_now(self) -> None:
        """현재 구독 집합 전체를 한 배열로 보낸다"""
        with self._lock:
            self._pending_declare = None
        declaration = self.build_declaration()
        if len(declaration) <= 1:
            return  # id 만 있으면 보낼 게 없다
        self.send(json.dumps(declaration))

    def build_declaration(self) -> list[dict]:
        with self._lock:
            self._request_counter += 1
            items: list[dict] = [{"id": f"req-{self._request_counter}"}]
            trade_keys = list(self._trade_listeners)
            book_keys = list(self._book_listeners)
            has_orders = bool(self._order_listeners)
            rejected = set(self._rejected)

        def codes_for(prefix: str, keys: list[str], market: str) -> list[str]:
            return sorted(k.split(":", 1)[1] for k in keys
                          if k.startswith(f"{market}:") and f"{prefix}:{market}:{k.split(':', 1)[1]}" not in rejected)
        for market in ("kr", "us"):
            codes = codes_for("trade", trade_keys, market)
            if codes:
                items.append({"type": f"trade:{market}", "codes": codes})
        for market in ("kr", "us"):
            codes = codes_for("orderbook", book_keys, market)
            if codes:
                items.append({"type": f"orderbook:{market}", "codes": codes})
        if has_orders:
            try:
                seq = self._account_seq()
            except Exception as e:  # noqa: BLE001
                logger.warning("toss stream: accountSeq 조회 실패 - 주문 이벤트 구독 보류: %s", e)
                seq = None
            if seq and f"personal:order:{seq}" not in rejected:
                items.append({"type": "personal:order", "codes": [str(seq)]})
        return items

    # ------------------------------------------------------------------ frames

    def on_message(self, text: str) -> None:
        try:
            node = json.loads(text)
        except ValueError:
            return
        if not isinstance(node, dict):
            return
        kind = str(node.get("type") or "")
        if kind == "pong":
            return
        if kind == "subscriptions":
            self._on_subscriptions(node)
        elif kind == "error":
            error = node.get("error") or {}
            logger.warning("toss stream: error %s %s (id=%s)", error.get("code", ""), error.get("message", ""), node.get("id", ""))
        elif kind == "message":
            self._on_data(str(node.get("topic") or ""), node.get("data") or {})
        else:
            logger.debug("toss stream: %s", text[:200])

    def _on_subscriptions(self, node: dict) -> None:
        subscribed = node.get("subscribed") or []
        logger.info("toss stream: subscribed %d (id=%s)", len(subscribed), node.get("id", ""))
        for r in node.get("rejected") or []:
            target = str(r.get("target") or "")
            logger.warning("toss stream: 구독 거부 %s %s %s - 선언에서 제외한다", target, r.get("code", ""), r.get("message", ""))
            if target:
                with self._lock:
                    self._rejected.add(target)

    def _on_data(self, topic: str, data: dict) -> None:
        parts = topic.split(":", 2)
        if len(parts) < 3:
            return
        key = f"{parts[1]}:{parts[2]}"
        if parts[0] == "trade":
            tick = parse_toss_trade(topic, data)
            if tick is not None:
                self._deliver(tick, key, self._trade_listeners, "리스너")
        elif parts[0] == "orderbook":
            book = parse_toss_order_book(topic, data)
            if book is not None:
                self._deliver(book, key, self._book_listeners, "호가 리스너")
        elif parts[0] == "personal":
            order_id = str((data.get("order") or {}).get("orderId") or "")
            event = parse_toss_order_event(data, self._filled_so_far.get(order_id))
            if event is None:
                return
            if self._usage is not None:
                self._usage.stream_message(StreamChannel.ORDER_EVENTS)
            filled = _dec((data.get("order") or {}).get("execution") or {}, "filledQuantity")
            if filled is not None:
                self._filled_so_far[order_id] = filled
            if event.type in (OrderEventType.CANCELED, OrderEventType.REJECTED):
                self._filled_so_far.pop(order_id, None)
            with self._lock:
                listeners = list(self._order_listeners)
            for listener in listeners:
                try:
                    listener(event)
                except Exception:  # noqa: BLE001
                    logger.exception("toss stream: 주문 이벤트 리스너 오류 / %s", event.order_id)

    def _deliver(self, tick, key: str, target: dict, label: str) -> None:
        if self._usage is not None:
            self._usage.stream_message(StreamChannel.ORDER_BOOK if isinstance(tick, OrderBookTick) else StreamChannel.TRADES)
        with self._lock:
            symbol = self._requested.get(key)
            listeners = list(target.get(key, ()))
        normalized = type(tick)(**{**tick.__dict__, "symbol": symbol}) if symbol and symbol != tick.symbol else tick
        for listener in listeners:
            try:
                listener(normalized)
            except Exception:  # noqa: BLE001
                logger.exception("toss stream: %s 오류 / %s", label, normalized.symbol)
