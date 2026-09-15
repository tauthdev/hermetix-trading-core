"""키움 REST API 실시간 스트림. 체결 0B·호가 0D 는 2026-09-14 모의투자 서버(mockapi) 장중 실측 통과(00 등록도 return_code 0),
주문체결 00 프레임은 문서 기반 (이 모의 계좌는 공매도 이수 전용이라 주문 불가 -> 통보 실측 전). Kotlin KiwoomMarketStream 과 동일 의미.

프로토콜:
- 접속: 모의 wss://mockapi.kiwoom.com:10000/api/dostk/websocket, 실전 wss://api.kiwoom.com:10000/...
- 로그인: 접속 직후 {"trnm":"LOGIN","token":<접근토큰>} -> {"trnm":"LOGIN","return_code":0,"sor_yn":"Y"}. REST 토큰을 그대로 쓴다
- 등록: {"trnm":"REG","grp_no":"1","refresh":"1","data":[{"item":[코드...],"type":["0B"]}]} -> {"trnm":"REG","return_code":0}.
  주문체결(00)은 계좌 단위라 item 을 빈 문자열 하나로 등록한다
- 데이터: {"data":[{"values":{...},"type":"0B","name":"주식체결","item":"005930"}],"trnm":"REAL"} (실측: data 가 trnm 앞)
- {"trnm":"PING"} 은 받은 그대로 되돌려 보낸다

FID:
- 0B 주식체결: 20 체결시각, 10 현재가, 11 전일대비, 12 등락율(%), 27 최우선매도호가, 28 최우선매수호가, 15 거래량(+매수/-매도), 13 누적거래량
- 0D 주식호가잔량: 21 호가시각, 41-50 매도호가1-10, 61-70 매도잔량1-10, 51-60 매수호가1-10, 71-80 매수잔량1-10, 121 총매도잔량, 125 총매수잔량
- 00 주문체결: 9203 주문번호, 904 원주문번호, 9001 종목코드(A접두), 913 주문상태(접수/체결/확인), 905 주문구분(+매수/-매도/매수취소...), 907 매도수구분(1 매도/2 매수),
  900 주문수량, 901 주문가격, 902 미체결수량, 910 체결가, 911 체결량, 908 주문/체결시각, 919 거부사유
REST 와 같이 가격·호가·수량에 등락 부호가 붙으므로 절대값으로 파싱한다. 실측 프레임은 conformance/fixtures/kiwoom.json#stream.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import date, datetime, time as dtime
from decimal import Decimal
from typing import Callable

from ..broker import KST, MarketStream, OrderBookListener, OrderEventListener, TradeListener
from ..models import OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType, OrderSide, TradeTick, symbol_code
from ..models import StreamChannel
from ..stream import ReconnectingWebSocket

logger = logging.getLogger("hermetix")

TYPE_TRADE = "0B"
TYPE_ORDER_BOOK = "0D"
TYPE_ORDER_EVENTS = "00"


def _signed(values: dict, fid: str) -> Decimal | None:
    text = str(values.get(fid, "")).strip().lstrip("+")
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def _kst(hhmmss: str, today: date) -> datetime:
    t = str(hhmmss).strip().rjust(6, "0")
    return datetime.combine(today, dtime(int(t[0:2]), int(t[2:4]), int(t[4:6])), tzinfo=KST)


def _code(item: dict) -> str:
    # 종목코드는 REST 와 같이 A 프리픽스가 붙어 올 수 있다 ("A005930")
    return str(item.get("item", "")).strip().lstrip("A")


def _items(obj: dict, kind: str) -> list[dict]:
    return [item for item in (obj.get("data") or []) if isinstance(item, dict) and item.get("type") == kind]


def parse_kiwoom_real(obj: dict, today: date | None = None) -> list[TradeTick]:
    """REAL 프레임 -> 체결 목록 (0B 만). 심볼은 종목코드 그대로 (요청 표기 복원은 스트림이 한다)."""
    today = today or datetime.now(KST).date()
    ticks = []
    for item in _items(obj, TYPE_TRADE):
        try:
            ticks.append(_parse_item(item, today))
        except Exception:  # noqa: BLE001 - 항목 단위 격리
            continue
    return ticks


def _parse_item(item: dict, today: date) -> TradeTick:
    v = item.get("values") or {}
    price = _signed(v, "10")
    if price is None:
        raise ValueError("no price")
    quantity = _signed(v, "15")
    cum = _signed(v, "13")
    rate = _signed(v, "12")
    ask, bid = _signed(v, "27"), _signed(v, "28")
    return TradeTick(
        symbol=_code(item),
        price=abs(price),
        quantity=abs(quantity) if quantity is not None else Decimal(0),
        timestamp=_kst(v.get("20", ""), today),
        ask_price=abs(ask) if ask is not None else None,
        bid_price=abs(bid) if bid is not None else None,
        cumulative_volume=int(abs(cum)) if cum is not None else None,
        change=_signed(v, "11"),
        change_rate=(rate / Decimal(100)).quantize(Decimal("0.000001")) if rate is not None else None,
    )


def parse_kiwoom_order_book(obj: dict, today: date | None = None) -> list[OrderBookTick]:
    """REAL 프레임 -> 호가창 목록 (0D 만). 0 호가는 빈 단계로 보고 뺀다"""
    today = today or datetime.now(KST).date()
    books = []
    for item in _items(obj, TYPE_ORDER_BOOK):
        try:
            v = item.get("values") or {}

            def levels(price_from: int, qty_from: int) -> list[OrderBookLevel]:
                out = []
                for i in range(10):
                    price = _signed(v, str(price_from + i))
                    if price is None or price == 0:
                        continue
                    qty = _signed(v, str(qty_from + i))
                    out.append(OrderBookLevel(abs(price), abs(qty) if qty is not None else Decimal(0)))
                return out
            total_ask, total_bid = _signed(v, "121"), _signed(v, "125")
            books.append(OrderBookTick(
                symbol=_code(item),
                timestamp=_kst(v.get("21", ""), today),
                asks=levels(41, 61),
                bids=levels(51, 71),
                total_ask_quantity=abs(total_ask) if total_ask is not None else None,
                total_bid_quantity=abs(total_bid) if total_bid is not None else None,
            ))
        except Exception:  # noqa: BLE001
            continue
    return books


def parse_kiwoom_order_events(obj: dict, today: date | None = None) -> list[OrderEvent]:
    """REAL 프레임 -> 주문 통보 목록 (00 만)"""
    today = today or datetime.now(KST).date()
    events = []
    for item in _items(obj, TYPE_ORDER_EVENTS):
        try:
            v = item.get("values") or {}
            status = str(v.get("913", "")).strip()
            kind_text = str(v.get("905", "")).strip()
            reason = str(v.get("919", "")).strip() or None
            filled_qty = _signed(v, "911")
            filled_qty = abs(filled_qty) if filled_qty is not None else None
            if reason is not None:
                kind = OrderEventType.REJECTED
            elif "체결" in status and filled_qty is not None and filled_qty > 0:
                kind = OrderEventType.FILLED
            elif "취소" in kind_text:
                kind = OrderEventType.CANCELED
            elif "정정" in kind_text:
                kind = OrderEventType.MODIFIED
            else:
                kind = OrderEventType.ACCEPTED
            original = str(v.get("904", "")).strip()
            if not original or not original.lstrip("0"):
                original = None
            qty, price = (_signed(v, "900"), _signed(v, "901"))
            fill_price = _signed(v, "910")
            remaining = _signed(v, "902")
            symbol = str(v.get("9001", "")).strip().lstrip("A") or None
            events.append(OrderEvent(
                order_id=str(v.get("9203", "")).strip(),
                type=kind,
                timestamp=_kst(v.get("908", "0"), today),
                symbol=symbol,
                side={"1": OrderSide.SELL, "2": OrderSide.BUY}.get(str(v.get("907", "")).strip()),
                quantity=filled_qty if kind == OrderEventType.FILLED else (abs(qty) if qty is not None else None),
                price=(abs(fill_price) if fill_price is not None else None) if kind == OrderEventType.FILLED
                else (abs(price) if price is not None else None),
                remaining_quantity=abs(remaining) if remaining is not None else None,
                original_order_id=original,
                reason=reason,
            ))
        except Exception:  # noqa: BLE001
            continue
    return events


class KiwoomMarketStream(ReconnectingWebSocket, MarketStream):

    def __init__(self, ws_url: str, token: Callable[[], str], usage=None):
        super().__init__("kiwoom", usage=usage)
        self._ws_url = ws_url
        self._token = token
        self._lock = threading.Lock()
        self._trade_listeners: dict[str, list[TradeListener]] = {}
        self._book_listeners: dict[str, list[OrderBookListener]] = {}
        self._order_listeners: list[OrderEventListener] = []
        self._requested: dict[str, str] = {}
        self._logged_in = False

    @property
    def is_connected(self) -> bool:
        return self.is_socket_open and self._logged_in

    def uri(self) -> str:
        return self._ws_url

    def on_open(self) -> None:
        self._logged_in = False
        self.send(json.dumps({"trnm": "LOGIN", "token": self._token()}))

    def on_disconnected(self) -> None:
        self._logged_in = False

    def _register(self, symbols: list[str], listener, target: dict) -> list[str]:
        new_codes = []
        with self._lock:
            for symbol in symbols:
                code = symbol_code(symbol)
                self._requested.setdefault(code, symbol)
                if code not in target:
                    target[code] = []
                    new_codes.append(code)
                target[code].append(listener)
        return new_codes

    def subscribe_trades(self, symbols: list[str], listener: TradeListener) -> None:
        new_codes = self._register(symbols, listener, self._trade_listeners)
        if new_codes and self._usage is not None:
            self._usage.stream_subscribed(StreamChannel.TRADES, len(new_codes))
        if new_codes and self.is_connected:
            self.send(self._register_message(new_codes, TYPE_TRADE))

    def subscribe_order_book(self, symbols: list[str], listener: OrderBookListener) -> None:
        new_codes = self._register(symbols, listener, self._book_listeners)
        if new_codes and self._usage is not None:
            self._usage.stream_subscribed(StreamChannel.ORDER_BOOK, len(new_codes))
        if new_codes and self.is_connected:
            self.send(self._register_message(new_codes, TYPE_ORDER_BOOK))

    def subscribe_order_events(self, listener: OrderEventListener) -> None:
        with self._lock:
            first = not self._order_listeners
            self._order_listeners.append(listener)
        if first and self._usage is not None:
            self._usage.stream_subscribed(StreamChannel.ORDER_EVENTS)
        if first and self.is_connected:
            self.send(self._register_message([""], TYPE_ORDER_EVENTS))

    def _register_all(self) -> None:
        with self._lock:
            trade_codes = list(self._trade_listeners)
            book_codes = list(self._book_listeners)
            has_orders = bool(self._order_listeners)
        if trade_codes:
            self.send(self._register_message(trade_codes, TYPE_TRADE))
        if book_codes:
            self.send(self._register_message(book_codes, TYPE_ORDER_BOOK))
        if has_orders:
            self.send(self._register_message([""], TYPE_ORDER_EVENTS))

    def on_message(self, text: str) -> None:
        try:
            node = json.loads(text)
        except ValueError:
            return
        if not isinstance(node, dict):
            return
        trnm = node.get("trnm", "")
        if trnm == "PING":
            self.send(text)
        elif trnm == "LOGIN":
            code = node.get("return_code", -1)
            if code == 0:
                self._logged_in = True
                logger.info("kiwoom stream: logged in")
                self._register_all()
            else:
                logger.error("kiwoom stream: 로그인 실패 return_code=%s %s", code, node.get("return_msg", ""))
        elif trnm == "REG":
            code = node.get("return_code", -1)
            if code == 0:
                logger.info("kiwoom stream: registered")
            else:
                logger.warning("kiwoom stream: 등록 실패 return_code=%s %s", code, node.get("return_msg", ""))
        elif trnm == "REAL":
            for tick in parse_kiwoom_real(node):
                self._deliver(tick, self._trade_listeners, "리스너")
            for book in parse_kiwoom_order_book(node):
                self._deliver(book, self._book_listeners, "호가 리스너")
            with self._lock:
                listeners = list(self._order_listeners)
            for event in parse_kiwoom_order_events(node):
                if self._usage is not None:
                    self._usage.stream_message(StreamChannel.ORDER_EVENTS)
                for listener in listeners:
                    try:
                        listener(event)
                    except Exception:  # noqa: BLE001
                        logger.exception("kiwoom stream: 주문 통보 리스너 오류 / %s", event.order_id)
        else:
            logger.debug("kiwoom stream: %s", text[:200])

    def _deliver(self, tick, target: dict, label: str) -> None:
        if self._usage is not None:
            self._usage.stream_message(StreamChannel.ORDER_BOOK if isinstance(tick, OrderBookTick) else StreamChannel.TRADES)
        code = tick.symbol
        with self._lock:
            symbol = self._requested.get(code, code)
            listeners = list(target.get(code, ()))
        normalized = tick if symbol == code else type(tick)(**{**tick.__dict__, "symbol": symbol})
        for listener in listeners:
            try:
                listener(normalized)
            except Exception:  # noqa: BLE001
                logger.exception("kiwoom stream: %s 오류 / %s", label, symbol)

    @staticmethod
    def _register_message(items: list[str], kind: str) -> str:
        return json.dumps({"trnm": "REG", "grp_no": "1", "refresh": "1",
                           "data": [{"item": items, "type": [kind]}]})
