"""NH PLUG 실시간 스트림. **문서 기반 구현, 실측 전** - 포털 openapi.json x-realtime-channels·API 가이드 DB·공식 Python SDK(nhplug-sdk) 에서 역추적.
Kotlin NhMarketStream 과 동일 의미.

프로토콜:
- 접속: 모의 wss://moapi.nhplug.com:17070/websocket, 운영 wss://api.nhplug.com:7070/websocket (경로 /websocket 필수). 핸드셰이크 헤더 없음
- 인증: 별도 로그인 프레임 없이 매 구독 메시지의 header.token (REST 접근토큰 그대로)
- 구독: {"header":{"token","tr_type":"1"|"2"},"body":{"tr_cd":<채널>,"tr_key":<종목코드>}}. 통보 채널은 tr_key 빈 문자열
- 채널: 체결 KRX oc / NXT nc / 통합 mc, 호가 ob / nb / mb - REST 와 달리 시장을 채널코드로 고른다 (market_cd 에 맞춤).
  통보는 체결 d2(체결·정정·취소·거부 결과) + 접수 d3(신규·정정·취소 접수), 둘 다 국내주식/국내파생 공용(itemgb 로 구분)
- 응답(ACK): {"header":{"tr_type","tr_cd","rsp_cd":"00000","rsp_msg"},"body":{"tr_key":[...]}} - 데이터 푸시 header 에는 rsp_cd/tr_type 이 없다
- 데이터: {"header":{"tr_cd","tr_key"},"body":{...}} (통보는 header 에 tr_key 없음). 값은 예시상 전부 문자열 - 숫자도 허용해 파싱
- heartbeat 없음(문서 명시) - 조용한 게 정상이므로 유휴 감시를 끈다. 암호화 없음

문서로 확정하지 못한 점 (실측 필요): sign/kospigb/janggubun/ordercd/order_type/procnm 코드값(sign 은 REST prdy_vrss_sign 과 같은 4/5/8/9 하락 가정),
movolume(이번 체결 수량)·new_volume(누적) 해석, orderno 와 REST mkt_orr_no 의 동일성(선행 0 무시 비교), 거부/정정 통보 순서, 모의 서버의 시세 채널 제공 여부.
픽스처(문서 재구성값): conformance/fixtures/nh.json#stream.
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
from ..stream import ReconnectingWebSocket

logger = logging.getLogger("hermetix")

ORDER_CHANNELS = ("d2", "d3")
TRADE_CHANNELS = ("oc", "nc", "mc")
BOOK_CHANNELS = ("ob", "nb", "mb")
FALLING_SIGNS = {"4", "5", "8", "9"}
_BOOK_PREFIXES = ("", "P_", "S_", "S4_", "S5_", "S6_", "S7_", "S8_", "S9_", "S10_")


def trade_channel(market_cd: str) -> str:
    """시장 구분에 따른 체결 채널 - KRX oc, NXT nc, UNT(통합) mc. tr_cd 는 대소문자를 구분하므로 정규화하지 않는다"""
    return {"NXT": "nc", "UNT": "mc"}.get(market_cd.upper(), "oc")


def book_channel(market_cd: str) -> str:
    return {"NXT": "nb", "UNT": "mb"}.get(market_cd.upper(), "ob")


def _dec(body: dict, field: str) -> Decimal | None:
    """문자열/숫자 어느 쪽으로 와도 파싱"""
    value = body.get(field)
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def _text(body: dict, field: str) -> str:
    value = body.get(field)
    return "" if value is None else str(value).strip()


def _kst(raw: str, today: date) -> datetime:
    """"HH:MM:SS" 또는 "HHMMSS" (KST, 날짜 없음 -> today)"""
    digits = "".join(ch for ch in raw if ch.isdigit()).rjust(6, "0")[:6]
    return datetime.combine(today, dtime(int(digits[0:2]), int(digits[2:4]), int(digits[4:6])), tzinfo=KST)


def normalize_code(raw: str) -> str:
    """A005930 / 12자리 표준코드 꼴을 6자리 단축코드로"""
    text = str(raw or "").strip().removeprefix("A")
    return text


def _symbol(node: dict, body: dict) -> str:
    key = _text(node.get("header") or {}, "tr_key")
    return key or _text(body, "code")


def parse_nh_trade(node: dict, today: date | None = None) -> list[TradeTick]:
    """체결 프레임(oc/nc/mc) -> 체결. 심볼은 header.tr_key 또는 body.code 그대로 (요청 표기 복원은 스트림이 한다)"""
    today = today or datetime.now(KST).date()
    body = node.get("body")
    if not isinstance(body, dict):
        return []
    try:
        sign = _text(body, "sign")
        change = _dec(body, "change")
        if change is not None and sign in FALLING_SIGNS and change > 0:
            change = -change
        rate = _dec(body, "chrate")
        if rate is not None and sign in FALLING_SIGNS and rate > 0:
            rate = -rate
        price = _dec(body, "price")
        if price is None:
            return []
        cumulative = _dec(body, "new_volume")
        if cumulative is None:
            cumulative = _dec(body, "volume")
        return [TradeTick(
            symbol=_symbol(node, body),
            price=price,
            quantity=_dec(body, "movolume") or Decimal(0),
            timestamp=_kst(_text(body, "time"), today),
            ask_price=_dec(body, "offer"),
            bid_price=_dec(body, "bid"),
            cumulative_volume=int(cumulative) if cumulative is not None else None,
            change=change,
            change_rate=(rate / Decimal(100)).quantize(Decimal("0.000001")) if rate is not None else None,
        )]
    except Exception:  # noqa: BLE001
        return []


def parse_nh_order_book(node: dict, today: date | None = None) -> list[OrderBookTick]:
    """호가 프레임(ob/nb/mb) -> 호가창. 1단계 접두 없음, 2단계 P_, 3단계 S_, 4~10단계 S4_~S10_. 0 호가는 뺀다"""
    today = today or datetime.now(KST).date()
    body = node.get("body")
    if not isinstance(body, dict):
        return []
    try:
        def levels(price_field: str, qty_field: str) -> list[OrderBookLevel]:
            out = []
            for p in _BOOK_PREFIXES:
                price = _dec(body, f"{p}{price_field}")
                if price is None or price == 0:
                    continue
                out.append(OrderBookLevel(price, _dec(body, f"{p}{qty_field}") or Decimal(0)))
            return out
        return [OrderBookTick(
            symbol=_symbol(node, body),
            timestamp=_kst(_text(body, "hotime"), today),
            asks=levels("offer", "offerrem"),
            bids=levels("bid", "bidrem"),
            total_ask_quantity=_dec(body, "T_offerrem"),
            total_bid_quantity=_dec(body, "T_bidrem"),
        )]
    except Exception:  # noqa: BLE001
        return []


def parse_nh_order_events(node: dict, today: date | None = None, account_no: str = "") -> list[OrderEvent]:
    """통보 프레임 -> 이벤트. d3(접수) -> ACCEPTED, d2 -> rejgb=1 REJECTED / ucgb 0 체결 FILLED, 1 정정 MODIFIED, 2 취소·3 효력해제 CANCELED.
    국내주식(itemgb=1)만, account_no 가 주어지면 계좌가 같은 것만."""
    today = today or datetime.now(KST).date()
    tr_cd = _text(node.get("header") or {}, "tr_cd")
    b = node.get("body")
    if not isinstance(b, dict) or tr_cd not in ORDER_CHANNELS:
        return []
    if _text(b, "itemgb") != "1":
        return []
    if account_no and _text(b, "accountno") and _text(b, "accountno") != account_no:
        return []
    try:
        side = {"1": OrderSide.SELL, "2": OrderSide.BUY}.get(_text(b, "slbygb"))
        original = _text(b, "orgordno")
        if not original or not original.lstrip("0"):
            original = None
        if tr_cd == "d3":
            return [OrderEvent(
                order_id=_text(b, "orderno"), type=OrderEventType.ACCEPTED, timestamp=_kst(_text(b, "order_time"), today),
                symbol=normalize_code(_text(b, "issuecd")), side=side,
                quantity=_dec(b, "ordergty"), price=_dec(b, "orderprc"), original_order_id=original,
            )]
        ucgb = _text(b, "ucgb")
        if _text(b, "rejgb") == "1":
            kind = OrderEventType.REJECTED
        elif ucgb == "1":
            kind = OrderEventType.MODIFIED
        elif ucgb in ("2", "3"):
            kind = OrderEventType.CANCELED
        else:
            kind = OrderEventType.FILLED
        return [OrderEvent(
            order_id=_text(b, "orderno"), type=kind, timestamp=_kst(_text(b, "conctime"), today),
            symbol=normalize_code(_text(b, "issuecd")), side=side,
            quantity=_dec(b, "concgty"), price=_dec(b, "concprc"),
        )]
    except Exception:  # noqa: BLE001
        return []


class NhMarketStream(ReconnectingWebSocket, MarketStream):

    def __init__(self, ws_url: str, token: Callable[[], str], market_cd: str = "KRX", account_no: str = ""):
        super().__init__("nh", idle_timeout_seconds=0)
        self._ws_url = ws_url
        self._token = token
        self._account_no = account_no
        self.trade_channel = trade_channel(market_cd)
        self.book_channel = book_channel(market_cd)
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
            self.send(self._message(t, "1", self.trade_channel, code))
        for code in book_codes:
            self.send(self._message(t, "1", self.book_channel, code))
        if has_orders:
            for ch in ORDER_CHANNELS:
                self.send(self._message(t, "1", ch, ""))

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
        if new_codes and self.is_socket_open:
            t = self._token()
            for code in new_codes:
                self.send(self._message(t, "1", self.trade_channel, code))

    def subscribe_order_book(self, symbols: list[str], listener: OrderBookListener) -> None:
        new_codes = self._register(symbols, listener, self._book_listeners)
        if new_codes and self.is_socket_open:
            t = self._token()
            for code in new_codes:
                self.send(self._message(t, "1", self.book_channel, code))

    def subscribe_order_events(self, listener: OrderEventListener) -> None:
        with self._lock:
            first = not self._order_listeners
            self._order_listeners.append(listener)
        if first and self.is_socket_open:
            t = self._token()
            for ch in ORDER_CHANNELS:
                self.send(self._message(t, "1", ch, ""))

    def on_message(self, text: str) -> None:
        try:
            node = json.loads(text)
        except ValueError:
            return
        if not isinstance(node, dict):
            return
        header = node.get("header") or {}
        tr_cd = str(header.get("tr_cd", ""))
        if "rsp_cd" in header or "tr_type" in header:
            rsp_cd = str(header.get("rsp_cd", ""))
            msg = str(header.get("rsp_msg", ""))
            if rsp_cd == "00000":
                verb = "unsubscribed" if str(header.get("tr_type", "")) == "2" else "subscribed"
                logger.info("nh stream: %s %s %s (%s)", verb, tr_cd, (node.get("body") or {}).get("tr_key"), msg)
            else:
                logger.warning("nh stream: %s rsp_cd=%s %s", tr_cd, rsp_cd, msg)
            return
        body = node.get("body")
        if not isinstance(body, dict):
            return
        if tr_cd in TRADE_CHANNELS:
            for tick in parse_nh_trade(node):
                self._deliver(tick, self._trade_listeners, "리스너")
        elif tr_cd in BOOK_CHANNELS:
            for book in parse_nh_order_book(node):
                self._deliver(book, self._book_listeners, "호가 리스너")
        elif tr_cd in ORDER_CHANNELS:
            with self._lock:
                listeners = list(self._order_listeners)
            for event in parse_nh_order_events(node, account_no=self._account_no):
                for listener in listeners:
                    try:
                        listener(event)
                    except Exception:  # noqa: BLE001
                        logger.exception("nh stream: 주문 통보 리스너 오류 / %s", event.order_id)
        else:
            logger.debug("nh stream: unknown tr_cd %s", tr_cd)

    def _deliver(self, tick, target: dict, label: str) -> None:
        code = tick.symbol
        with self._lock:
            symbol = self._requested.get(code, code)
            listeners = list(target.get(code, ()))
        normalized = tick if symbol == code else type(tick)(**{**tick.__dict__, "symbol": symbol})
        for listener in listeners:
            try:
                listener(normalized)
            except Exception:  # noqa: BLE001
                logger.exception("nh stream: %s 오류 / %s", label, symbol)

    @staticmethod
    def _message(token: str, tr_type: str, tr_cd: str, tr_key: str) -> str:
        return json.dumps({"header": {"token": token, "tr_type": tr_type}, "body": {"tr_cd": tr_cd, "tr_key": tr_key}})
