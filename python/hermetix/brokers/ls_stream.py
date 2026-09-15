"""LS증권 OPEN API 실시간 스트림. **문서 기반 구현, 모의 실측 전** - 포털 실시간 TR 문서와 커뮤니티 클라이언트(ebest·LsApiHelper·krsec)에서 역추적.
Kotlin LsMarketStream 과 동일 의미.

프로토콜:
- 접속: 모의 wss://openapi.ls-sec.co.kr:29443/websocket, 실전 :9443/websocket. 핸드셰이크 헤더·로그인 프레임 없음
- 인증: 매 메시지 header.token 에 REST 접근토큰(Bearer 접두 없음). 토큰은 익일 07:00 만료 -> 재접속 시 캐시 토큰을 다시 싣는다
- 시세 등록/해제: {"header":{"token","tr_type":"3"|"4"},"body":{"tr_cd":"S3_","tr_key":"005930"}}
- 계좌 등록/해제: tr_type "1"/"2", tr_cd SC0(접수)·SC1(체결)·SC2(정정)·SC3(취소)·SC4(거부), tr_key 빈 문자열. TR 마다 따로 보낸다
- 응답: 등록 ACK 는 header 에 rsp_cd/rsp_msg 가 있고 body 가 없다. 데이터는 {"header":{"tr_cd","tr_key"},"body":{...}}, 값은 전부 문자열
- 하트비트 없음(문서 미기재) - 유휴 감시를 끈다

KOSPI/KOSDAQ 선택: 서버는 종목으로 시장을 고르지 않는다 - KOSDAQ 코드를 S3_ 에 넣으면 아무것도 오지 않는다. 종목마스터(t8436) 조회 대신
종목마다 KOSPI TR(S3_/H1_)과 KOSDAQ TR(K3_/HA_)을 둘 다 등록한다. 맞지 않는 쪽은 조용히 비고, 등록 수만 2배가 된다 (한도 미문서).

실측 시 확인할 것: ACK JSON 키·rsp_cd 값, SC4 본문·거부 사유, 세션당 등록 한도, 앱키당 세션 수, 07:00 토큰 만료 시 소켓 동작.
픽스처(문서 재구성값): conformance/fixtures/ls.json#stream.
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

TR_TRADE_KOSPI = "S3_"
TR_TRADE_KOSDAQ = "K3_"
TR_BOOK_KOSPI = "H1_"
TR_BOOK_KOSDAQ = "HA_"
TR_ORDER_ACCEPTED = "SC0"
TR_ORDER_FILLED = "SC1"
TR_ORDER_MODIFIED = "SC2"
TR_ORDER_CANCELED = "SC3"
TR_ORDER_REJECTED = "SC4"
TRADE_TRS = (TR_TRADE_KOSPI, TR_TRADE_KOSDAQ)
BOOK_TRS = (TR_BOOK_KOSPI, TR_BOOK_KOSDAQ)
ORDER_TRS = (TR_ORDER_ACCEPTED, TR_ORDER_FILLED, TR_ORDER_MODIFIED, TR_ORDER_CANCELED, TR_ORDER_REJECTED)

TR_TYPE_ACCOUNT_REGISTER = "1"
TR_TYPE_ACCOUNT_UNREGISTER = "2"
TR_TYPE_SUBSCRIBE = "3"
TR_TYPE_UNSUBSCRIBE = "4"
RSP_OK = "00000"
# xingAPI 관례 sign: 1 상한 2 상승 3 보합 4 하한 5 하락
_FALLING_SIGNS = {"4", "5"}


def _text(body: dict, field: str) -> str:
    value = body.get(field)
    return "" if value is None else str(value).strip()


def _dec(body: dict, field: str) -> Decimal | None:
    text = _text(body, field).replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def _kst(raw: str, today: date) -> datetime:
    """HHMMSS 또는 HHMMSSmmm -> 오늘 KST 시각"""
    t = raw.strip().rjust(6, "0")[:6]
    return datetime.combine(today, dtime(int(t[0:2]), int(t[2:4]), int(t[4:6])), tzinfo=KST)


def normalize_code(raw: str) -> str:
    """A005930 -> 005930"""
    return str(raw or "").strip().removeprefix("A")


def _is_control(node: dict) -> bool:
    """ACK/오류 프레임(header 에 rsp_*·body 없음)이면 True - 데이터 파서는 건너뛴다"""
    header = node.get("header") or {}
    body = node.get("body")
    return "rsp_msg" in header or "rsp_cd" in header or not isinstance(body, dict) or not body


def _market_code(node: dict) -> str:
    """시세 프레임의 종목코드 - header.tr_key 6자리, 없으면 body.shcode"""
    key = _text(node.get("header") or {}, "tr_key")
    return key if len(key) == 6 else _text(node.get("body") or {}, "shcode")


def parse_ls_trade(node: dict, today: date | None = None) -> TradeTick | None:
    """체결 프레임(S3_/K3_) -> 체결. 심볼은 단축코드 그대로 (요청 표기 복원은 스트림이 한다). TR 불일치·필수 필드 부족이면 None"""
    today = today or datetime.now(KST).date()
    if _is_control(node) or _text(node.get("header") or {}, "tr_cd") not in TRADE_TRS:
        return None
    b = node["body"]
    try:
        sign = _text(b, "sign")
        change = _dec(b, "change")
        if change is not None and sign in _FALLING_SIGNS and change > 0:
            change = -change
        price = _dec(b, "price")
        if price is None:
            return None
        rate = _dec(b, "drate")
        cumulative = _dec(b, "volume")
        return TradeTick(
            symbol=_market_code(node),
            price=price,
            quantity=_dec(b, "cvolume") or Decimal(0),
            timestamp=_kst(_text(b, "chetime"), today),
            ask_price=_dec(b, "offerho"),
            bid_price=_dec(b, "bidho"),
            cumulative_volume=int(cumulative) if cumulative is not None else None,
            change=change,
            change_rate=(rate / Decimal(100)).quantize(Decimal("0.000001")) if rate is not None else None,
        )
    except Exception:  # noqa: BLE001
        return None


def parse_ls_order_book(node: dict, today: date | None = None) -> OrderBookTick | None:
    """호가 프레임(H1_/HA_) -> 호가창. 0 호가는 빈 단계로 보고 뺀다"""
    today = today or datetime.now(KST).date()
    if _is_control(node) or _text(node.get("header") or {}, "tr_cd") not in BOOK_TRS:
        return None
    b = node["body"]
    try:
        def levels(price_prefix: str, qty_prefix: str) -> list[OrderBookLevel]:
            out = []
            for i in range(1, 11):
                price = _dec(b, f"{price_prefix}{i}")
                if price is None or price == 0:
                    continue
                out.append(OrderBookLevel(price, _dec(b, f"{qty_prefix}{i}") or Decimal(0)))
            return out
        return OrderBookTick(
            symbol=_market_code(node),
            timestamp=_kst(_text(b, "hotime"), today),
            asks=levels("offerho", "offerrem"),
            bids=levels("bidho", "bidrem"),
            total_ask_quantity=_dec(b, "totofferrem"),
            total_bid_quantity=_dec(b, "totbidrem"),
        )
    except Exception:  # noqa: BLE001
        return None


def parse_ls_order_events(node: dict, today: date | None = None) -> list[OrderEvent]:
    """주문 통보 프레임(SC0~SC4) -> 이벤트 목록 (프레임당 1건, 파싱 실패면 빈 목록)"""
    today = today or datetime.now(KST).date()
    tr_cd = _text(node.get("header") or {}, "tr_cd")
    if _is_control(node) or tr_cd not in ORDER_TRS:
        return []
    b = node["body"]
    try:
        order_id = _text(b, "ordno")
        if not order_id:
            return []
        original = _text(b, "orgordno")
        if not original or not original.lstrip("0") or original == order_id:
            original = None
        side = {"1": OrderSide.SELL, "2": OrderSide.BUY}.get(_text(b, "bnstp"))
        symbol = normalize_code(_text(b, "shtnIsuno") or _text(b, "shtcode")) or None
        code = _text(b, "ordxctptncode")
        by_code = {"11": OrderEventType.FILLED, "12": OrderEventType.MODIFIED, "13": OrderEventType.CANCELED, "14": OrderEventType.REJECTED}
        by_tr = {TR_ORDER_FILLED: OrderEventType.FILLED, TR_ORDER_MODIFIED: OrderEventType.MODIFIED,
                 TR_ORDER_CANCELED: OrderEventType.CANCELED, TR_ORDER_REJECTED: OrderEventType.REJECTED}
        kind = by_code.get(code) or by_tr.get(tr_cd, OrderEventType.ACCEPTED)
        if kind == OrderEventType.ACCEPTED:
            quantity, price = _dec(b, "ordqty"), _dec(b, "ordprice")
        elif kind == OrderEventType.FILLED:
            quantity, price = _dec(b, "execqty"), _dec(b, "execprc")
        elif kind == OrderEventType.MODIFIED:
            quantity, price = _dec(b, "mdfycnfqty"), _dec(b, "mdfycnfprc")
        elif kind == OrderEventType.CANCELED:
            quantity, price = _dec(b, "canccnfqty"), _dec(b, "ordprc")
        else:
            quantity, price = _dec(b, "rjtqty"), _dec(b, "ordprc")
        time_text = _text(b, "ordtm") if tr_cd == TR_ORDER_ACCEPTED else _text(b, "exectime")
        return [OrderEvent(
            order_id=order_id,
            type=kind,
            timestamp=_kst(time_text, today),
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            remaining_quantity=None if tr_cd == TR_ORDER_ACCEPTED else _dec(b, "unercqty"),
            original_order_id=original,
            reason=(_text(b, "msgcode") or None) if kind == OrderEventType.REJECTED else None,
        )]
    except Exception:  # noqa: BLE001
        return []


class LsMarketStream(ReconnectingWebSocket, MarketStream):

    def __init__(self, ws_url: str, token: Callable[[], str], usage=None):
        super().__init__("ls", idle_timeout_seconds=0, usage=usage)
        self._ws_url = ws_url
        self._token = token
        self._lock = threading.Lock()
        self._trade_listeners: dict[str, list[TradeListener]] = {}
        self._book_listeners: dict[str, list[OrderBookListener]] = {}
        self._order_listeners: list[OrderEventListener] = []
        self._requested: dict[str, str] = {}

    @property
    def is_connected(self) -> bool:
        return self.is_socket_open

    def uri(self) -> str:
        return self._ws_url

    def on_open(self) -> None:
        t = self._token()
        with self._lock:
            trade_codes = list(self._trade_listeners)
            book_codes = list(self._book_listeners)
            has_orders = bool(self._order_listeners)
        for code in trade_codes:
            for tr in TRADE_TRS:
                self.send(self._message(t, TR_TYPE_SUBSCRIBE, tr, code))
        for code in book_codes:
            for tr in BOOK_TRS:
                self.send(self._message(t, TR_TYPE_SUBSCRIBE, tr, code))
        if has_orders:
            for tr in ORDER_TRS:
                self.send(self._message(t, TR_TYPE_ACCOUNT_REGISTER, tr, ""))

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
        if new_codes and self.is_socket_open:
            t = self._token()
            for code in new_codes:
                for tr in TRADE_TRS:
                    self.send(self._message(t, TR_TYPE_SUBSCRIBE, tr, code))

    def subscribe_order_book(self, symbols: list[str], listener: OrderBookListener) -> None:
        new_codes = self._register(symbols, listener, self._book_listeners)
        if new_codes and self._usage is not None:
            self._usage.stream_subscribed(StreamChannel.ORDER_BOOK, len(new_codes))
        if new_codes and self.is_socket_open:
            t = self._token()
            for code in new_codes:
                for tr in BOOK_TRS:
                    self.send(self._message(t, TR_TYPE_SUBSCRIBE, tr, code))

    def subscribe_order_events(self, listener: OrderEventListener) -> None:
        with self._lock:
            first = not self._order_listeners
            self._order_listeners.append(listener)
        if first and self._usage is not None:
            self._usage.stream_subscribed(StreamChannel.ORDER_EVENTS)
        if first and self.is_socket_open:
            t = self._token()
            for tr in ORDER_TRS:
                self.send(self._message(t, TR_TYPE_ACCOUNT_REGISTER, tr, ""))

    def unsubscribe_trades(self, symbols: list[str]) -> None:
        """체결 구독 해제 (tr_type 4). 리스너도 지운다"""
        with self._lock:
            codes = [c for c in (symbol_code(s) for s in symbols) if self._trade_listeners.pop(c, None) is not None]
        if codes and self.is_socket_open:
            t = self._token()
            for code in codes:
                for tr in TRADE_TRS:
                    self.send(self._message(t, TR_TYPE_UNSUBSCRIBE, tr, code))

    def unsubscribe_order_book(self, symbols: list[str]) -> None:
        """호가 구독 해제 (tr_type 4). 리스너도 지운다"""
        with self._lock:
            codes = [c for c in (symbol_code(s) for s in symbols) if self._book_listeners.pop(c, None) is not None]
        if codes and self.is_socket_open:
            t = self._token()
            for code in codes:
                for tr in BOOK_TRS:
                    self.send(self._message(t, TR_TYPE_UNSUBSCRIBE, tr, code))

    def unsubscribe_order_events(self) -> None:
        """계좌 통보 해제 (tr_type 2). 리스너도 지운다"""
        with self._lock:
            if not self._order_listeners:
                return
            self._order_listeners.clear()
        if self.is_socket_open:
            t = self._token()
            for tr in ORDER_TRS:
                self.send(self._message(t, TR_TYPE_ACCOUNT_UNREGISTER, tr, ""))

    def on_message(self, text: str) -> None:
        try:
            node = json.loads(text)
        except ValueError:
            return
        if not isinstance(node, dict):
            return
        header = node.get("header") or {}
        tr_cd = str(header.get("tr_cd", ""))
        body = node.get("body")
        if "rsp_msg" in header or "rsp_cd" in header or not isinstance(body, dict):
            rsp_cd = str(header.get("rsp_cd", ""))
            msg = str(header.get("rsp_msg", ""))
            if not rsp_cd or rsp_cd == RSP_OK:
                logger.info("ls stream: ack %s %s tr_type=%s (%s)", tr_cd, header.get("tr_key", ""), header.get("tr_type", ""), msg)
            else:
                logger.warning("ls stream: %s %s rsp_cd=%s %s", tr_cd, header.get("tr_key", ""), rsp_cd, msg)
            return
        if tr_cd in TRADE_TRS:
            tick = parse_ls_trade(node)
            if tick is not None:
                self._deliver(tick, self._trade_listeners, "리스너")
        elif tr_cd in BOOK_TRS:
            book = parse_ls_order_book(node)
            if book is not None:
                self._deliver(book, self._book_listeners, "호가 리스너")
        elif tr_cd in ORDER_TRS:
            with self._lock:
                listeners = list(self._order_listeners)
            for event in parse_ls_order_events(node):
                if self._usage is not None:
                    self._usage.stream_message(StreamChannel.ORDER_EVENTS)
                for listener in listeners:
                    try:
                        listener(event)
                    except Exception:  # noqa: BLE001
                        logger.exception("ls stream: 주문 통보 리스너 오류 / %s", event.order_id)
        else:
            logger.debug("ls stream: unknown tr %s", tr_cd)

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
                logger.exception("ls stream: %s 오류 / %s", label, symbol)

    @staticmethod
    def _message(token: str, tr_type: str, tr_cd: str, tr_key: str) -> str:
        return json.dumps({"header": {"token": token, "tr_type": tr_type}, "body": {"tr_cd": tr_cd, "tr_key": tr_key}})
