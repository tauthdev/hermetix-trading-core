"""DB증권 실시간 스트림 - 체결 S00, 호가 S01, 주문 접수 IS0, 주문 체결 IS1. **문서 기반 구현, 실측 전.** Kotlin DbMarketStream 과 동일 의미.

프로토콜 (포털 [실시간] 문서 + 공식 SDK DBsecurities/dbsec-open-api):
- 접속: 운영 wss://openapi.dbsec.co.kr:7070/websocket, 모의 :17070/websocket. 핸드셰이크 헤더 없음
- 인증: 로그인 프레임 없이 매 메시지 header.token 에 REST 접근토큰(Bearer 접두 없음)
- 시세 등록 {"header":{"token","tr_type":"1"},"body":{"tr_cd":"S00","tr_key":"J 005930"}}, 해제는 tr_type:"2".
  tr_key = 시장구분 2자리(J + 공백) + 종목코드. NXT 는 NJN-005930, 통합은 UJU-005930 (미지원)
- 계좌 등록 {"header":{"token","tr_type":"3"},"body":{"tr_cd":"IS0"}} - tr_key 없음, 해제 메시지 없음(세션 종료가 해제)
- 접속 후 10초 안에 첫 메시지를 보내야 한다 -> 구독이 있으면 on_open 에서 즉시 전송. 서버 주기 프레임이 없어 유휴 감시는 끈다
- 응답: 구독 ack {"header":{"tr_cd":"S00","tr_key":null},"body":{"tr_key":["J 005930"]}}, 오류·제어 프레임은 header/body 가 null 이거나
  rsp_cd/rsp_msg 를 어느 쪽에든 싣는다(""/"0"/"00000" 가 정상). 데이터 프레임은 header.tr_cd 로 라우팅하고
  header.tr_key 는 항상 null 이라 심볼은 body(ShrnIscd/Sshtnisuno)에서 읽는다
- body 값은 모두 문자열(계좌계는 0 패딩). 필드명 대소문자가 문서와 예시에서 다르다(askp1 vs Askp1) -> 대소문자 무시 조회

Sordxctptncode(주문체결유형코드) 값표는 미공개라 상태는 수량 필드로 판정한다. 픽스처: conformance/fixtures/db.json#stream.
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

TR_TRADE = "S00"
TR_ORDER_BOOK = "S01"
TR_ORDER_ACCEPTED = "IS0"
TR_ORDER_EXECUTED = "IS1"
# KRX 주식/ETF 시장구분 2자리 - J + 공백. NXT 는 NJN-, 통합은 UJU- (미지원)
MARKET_PREFIX = "J "
_SUCCESS_CODES = {"", "0", "00000"}


def _lower(body: dict) -> dict:
    """문서 표(askp1)와 예시(Askp1)의 대소문자가 달라 소문자 키로 조회한다"""
    return {str(k).lower(): v for k, v in body.items()}


def _text(f: dict, field: str) -> str:
    value = f.get(field.lower())
    return "" if value is None else str(value).strip()


def _dec(f: dict, field: str) -> Decimal | None:
    text = _text(f, field).replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def _kst(hhmmss: str, today: date) -> datetime:
    t = hhmmss.strip()[:6].rjust(6, "0")
    return datetime.combine(today, dtime(int(t[0:2]), int(t[2:4]), int(t[4:6])), tzinfo=KST)


def normalize_code(raw: str) -> str:
    """U-005930 / N-005930 / A005930 -> 005930"""
    return str(raw or "").strip().removeprefix("U-").removeprefix("N-").removeprefix("A")


def parse_db_trade(body: dict, today: date | None = None) -> TradeTick | None:
    """S00 body -> 체결. 심볼은 단축코드(요청 표기 복원은 스트림이 한다). 필드 부족이면 None"""
    today = today or datetime.now(KST).date()
    try:
        f = _lower(body)
        change = _dec(f, "PrdyVrss")
        falling = _text(f, "PrdyVrssclr") == "-" or _text(f, "PrdyVrsssign") in ("4", "5")
        if change is not None and falling and change > 0:
            change = -change
        price = _dec(f, "StckPrpr")
        if price is None:
            return None
        rate = _dec(f, "PrdyCtrt")
        cumulative = _dec(f, "AcmlVol")
        return TradeTick(
            symbol=normalize_code(_text(f, "ShrnIscd")),
            price=price,
            quantity=_dec(f, "CntgVol") or Decimal(0),
            timestamp=_kst(_text(f, "StckCntghour"), today),
            ask_price=_dec(f, "askp1"),
            bid_price=_dec(f, "bidp1"),
            cumulative_volume=int(cumulative) if cumulative is not None else None,
            change=change,
            change_rate=(rate / Decimal(100)).quantize(Decimal("0.000001")) if rate is not None else None,
        )
    except Exception:  # noqa: BLE001
        return None


def parse_db_order_book(body: dict, today: date | None = None) -> OrderBookTick | None:
    """S01 body -> 호가창(10단계, 0 호가는 제외)"""
    today = today or datetime.now(KST).date()
    try:
        f = _lower(body)

        def levels(price_field: str, qty_field: str) -> list[OrderBookLevel]:
            out = []
            for i in range(1, 11):
                price = _dec(f, f"{price_field}{i}")
                if price is None or price == 0:
                    continue
                out.append(OrderBookLevel(price, _dec(f, f"{qty_field}{i}") or Decimal(0)))
            return out
        return OrderBookTick(
            symbol=normalize_code(_text(f, "ShrnIscd")),
            timestamp=_kst(_text(f, "BsopHour"), today),
            asks=levels("askp", "AskpRsqn"),
            bids=levels("bidp", "BidpRsqn"),
            total_ask_quantity=_dec(f, "TotalAskprsqn"),
            total_bid_quantity=_dec(f, "TotalBidprsqn"),
        )
    except Exception:  # noqa: BLE001
        return None


def parse_db_order_event(tr_cd: str, body: dict, today: date | None = None) -> OrderEvent | None:
    """IS0(접수) / IS1(체결·정정·취소·거부) body -> 주문 통보. 유형코드 값표가 없어 수량 필드로 판정한다:
    거부수량 > 0 -> REJECTED, 체결수량 > 0 -> FILLED, 취소확인수량 > 0 -> CANCELED, 정정확인수량 > 0 -> MODIFIED, 그 외 ACCEPTED"""
    today = today or datetime.now(KST).date()
    try:
        f = _lower(body)
        order_id = _text(f, "Sordno")
        if not order_id:
            return None
        original = _text(f, "Sorgordno")
        if not original or not original.lstrip("0") or original.lstrip("0") == order_id.lstrip("0"):
            original = None
        side = {"1": OrderSide.SELL, "2": OrderSide.BUY}.get(_text(f, "Sbnstp"))
        symbol = normalize_code(_text(f, "Sshtnisuno")) or None
        if tr_cd == TR_ORDER_ACCEPTED:
            return OrderEvent(
                order_id=order_id, type=OrderEventType.ACCEPTED, timestamp=_kst(_text(f, "Sordtm"), today),
                symbol=symbol, side=side, quantity=_dec(f, "Sordqty"), price=_dec(f, "Sordprc"), original_order_id=original,
            )

        def positive(field: str) -> bool:
            v = _dec(f, field)
            return v is not None and v > 0
        if positive("Srjtqty"):
            kind, quantity, price = OrderEventType.REJECTED, _dec(f, "Srjtqty"), _dec(f, "Sordprc")
        elif positive("Sexecqty"):
            kind, quantity, price = OrderEventType.FILLED, _dec(f, "Sexecqty"), _dec(f, "Sexecprc")
        elif positive("Scanccnfqty"):
            kind, quantity, price = OrderEventType.CANCELED, _dec(f, "Scanccnfqty"), _dec(f, "Sordprc")
        elif positive("Smdfycnfqty"):
            kind, quantity, price = OrderEventType.MODIFIED, _dec(f, "Smdfycnfqty"), _dec(f, "Smdfycnfprc")
        else:
            kind, quantity, price = OrderEventType.ACCEPTED, _dec(f, "Sordqty"), _dec(f, "Sordprc")
        time_text = _text(f, "Sexectime") or _text(f, "Sordtm")
        return OrderEvent(
            order_id=order_id, type=kind, timestamp=_kst(time_text, today),
            symbol=symbol, side=side, quantity=quantity, price=price,
            remaining_quantity=_dec(f, "Sunercqty"), original_order_id=original,
        )
    except Exception:  # noqa: BLE001
        return None


class DbMarketStream(ReconnectingWebSocket, MarketStream):

    def __init__(self, ws_url: str, token: Callable[[], str]):
        super().__init__("db", idle_timeout_seconds=0)
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
        """접속 직후 전부 다시 보낸다 - 10초 규칙 때문에 지체하지 않는다"""
        t = self._token()
        with self._lock:
            trade_codes = list(self._trade_listeners)
            book_codes = list(self._book_listeners)
            has_orders = bool(self._order_listeners)
        for code in trade_codes:
            self.send(self._quote_message(t, TR_TRADE, code, "1"))
        for code in book_codes:
            self.send(self._quote_message(t, TR_ORDER_BOOK, code, "1"))
        if has_orders:
            self._send_account_registrations(t)

    def _send_account_registrations(self, t: str) -> None:
        self.send(self._account_message(t, TR_ORDER_ACCEPTED))
        self.send(self._account_message(t, TR_ORDER_EXECUTED))

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
                self.send(self._quote_message(t, TR_TRADE, code, "1"))

    def subscribe_order_book(self, symbols: list[str], listener: OrderBookListener) -> None:
        new_codes = self._register(symbols, listener, self._book_listeners)
        if new_codes and self.is_socket_open:
            t = self._token()
            for code in new_codes:
                self.send(self._quote_message(t, TR_ORDER_BOOK, code, "1"))

    def subscribe_order_events(self, listener: OrderEventListener) -> None:
        with self._lock:
            first = not self._order_listeners
            self._order_listeners.append(listener)
        if first and self.is_socket_open:
            self._send_account_registrations(self._token())

    def on_message(self, text: str) -> None:
        try:
            node = json.loads(text)
        except ValueError:
            return
        if not isinstance(node, dict):
            return
        header = node.get("header")
        body = node.get("body")
        header = header if isinstance(header, dict) else {}
        body_dict = body if isinstance(body, dict) else {}
        # 제어 프레임: rsp_cd/rsp_msg 가 header 또는 body 에 실린다. header/body 둘 다 비면 keepalive 류 - 무시
        rsp_cd = str(header.get("rsp_cd", body_dict.get("rsp_cd", "")) or "")
        rsp_msg = str(header.get("rsp_msg", body_dict.get("rsp_msg", "")) or "")
        tr_cd = str(header.get("tr_cd", ""))
        if rsp_cd or rsp_msg:
            if rsp_cd in _SUCCESS_CODES:
                logger.info("db stream: %s %s", tr_cd, rsp_msg)
            else:
                logger.warning("db stream: %s rsp_cd=%s %s", tr_cd, rsp_cd, rsp_msg)
            return
        if not header or not isinstance(body, dict):
            return
        if isinstance(body.get("tr_key"), list):  # 구독 ack
            logger.info("db stream: subscribed %s %s", tr_cd, body.get("tr_key"))
            return
        if tr_cd == TR_TRADE:
            tick = parse_db_trade(body)
            if tick is not None:
                self._deliver(tick, self._trade_listeners, "리스너")
        elif tr_cd == TR_ORDER_BOOK:
            book = parse_db_order_book(body)
            if book is not None:
                self._deliver(book, self._book_listeners, "호가 리스너")
        elif tr_cd in (TR_ORDER_ACCEPTED, TR_ORDER_EXECUTED):
            event = parse_db_order_event(tr_cd, body)
            if event is None:
                return
            with self._lock:
                listeners = list(self._order_listeners)
            for listener in listeners:
                try:
                    listener(event)
                except Exception:  # noqa: BLE001
                    logger.exception("db stream: 주문 통보 리스너 오류 / %s", event.order_id)
        else:
            logger.debug("db stream: unknown tr %s", tr_cd)

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
                logger.exception("db stream: %s 오류 / %s", label, symbol)

    @staticmethod
    def _quote_message(t: str, tr_cd: str, code: str, tr_type: str) -> str:
        return json.dumps({"header": {"token": t, "tr_type": tr_type}, "body": {"tr_cd": tr_cd, "tr_key": MARKET_PREFIX + code}})

    @staticmethod
    def _account_message(t: str, tr_cd: str) -> str:
        return json.dumps({"header": {"token": t, "tr_type": "3"}, "body": {"tr_cd": tr_cd}})
