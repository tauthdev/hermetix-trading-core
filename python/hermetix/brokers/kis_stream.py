"""KIS 실시간 스트림. 체결가 H0STCNT0·호가 H0STASP0 는 2026-09-14 모의투자 서버(ops...:31000) 장중 실측 통과,
주문 통보 H0STCNI9(모의)/H0STCNI0(실전)는 문서 기반 (HTS ID 로 구독해야 실측 가능). Kotlin KisMarketStream 과 동일 의미.

프로토콜:
- 접속: 모의 ws://ops.koreainvestment.com:31000, 실전 :21000. TLS 없음
- 인증: REST POST /oauth2/Approval 로 받은 approval_key 를 구독 메시지 헤더에 싣는다 (매 접속마다 재발급 가능)
- 구독: {"header":{"approval_key","custtype","tr_type":"1","content-type":"utf-8"},"body":{"input":{"tr_id","tr_key"}}}
  tr_key 는 시세 TR 이면 종목코드, 주문 통보 TR 이면 HTS ID
- 데이터 프레임: 0|TR|<건수>|<필드^필드^...> - 첫 세그먼트 0 은 평문, 1 은 AES 암호문(base64).
  레코드가 여러 건이면 본문에 이어 붙는다 (필드 폭 = 전체 필드 수 / 건수). 실측: 체결가는 한 프레임에 최대 3건
- 제어 프레임(JSON): 구독 결과(body.rt_cd/msg_cd, 암호화 TR 이면 output.iv/key), PINGPONG(그대로 되돌려 보내야 연결 유지)

필드 순서:
- H0STCNT0 (폭 47): 0 코드, 1 시각 HHMMSS, 2 현재가, 3 전일대비부호, 4 전일대비(부호 포함), 5 전일대비율(%), ... 10 매도호가1, 11 매수호가1, 12 체결량, 13 누적거래량
- H0STASP0 (실측 폭 63): 0 코드, 1 시각, 2 시간구분, 3-12 매도호가1-10, 13-22 매수호가1-10, 23-32 매도잔량1-10, 33-42 매수잔량1-10, 43 총매도잔량, 44 총매수잔량
- H0STCNI9/0 (AES-256-CBC, 구독 응답의 key/iv): 0 고객ID, 1 계좌번호, 2 주문번호, 3 원주문번호, 4 매도매수구분(01 매도/02 매수), 5 정정취소구분(0/1 정정/2 취소),
  6 주문종류, 7 주문조건, 8 종목코드, 9 체결수량, 10 체결단가, 11 체결시각, 12 거부여부(0/1), 13 체결여부(1 접수·정정·취소·거부 / 2 체결), 14 접수여부, 15 지점, 16 주문수량, ... 25 주문가격

실측 프레임은 conformance/fixtures/kis.json#stream.
"""
from __future__ import annotations

import base64
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

TR_TRADE = "H0STCNT0"
TR_ORDER_BOOK = "H0STASP0"
TR_ORDER_EVENTS_PAPER = "H0STCNI9"
TR_ORDER_EVENTS_LIVE = "H0STCNI0"
_MIN_TRADE_FIELDS = 14
_MIN_BOOK_FIELDS = 45
_MIN_ORDER_FIELDS = 17


def _dec(text: str) -> Decimal | None:
    try:
        return Decimal(text.strip()) if text.strip() else None
    except Exception:
        return None


def _hhmmss(text: str, today: date) -> datetime:
    t = text.strip().rjust(6, "0")
    return datetime.combine(today, dtime(int(t[0:2]), int(t[2:4]), int(t[4:6])), tzinfo=KST)


def _records(frame: str, tr_id: str, min_fields: int) -> list[list[str]]:
    """0|TR|건수|본문 을 레코드(필드 목록)로 나눈다. TR 불일치·필드 부족이면 빈 목록"""
    parts = frame.split("|", 3)
    if len(parts) < 4 or parts[1] != tr_id:
        return []
    try:
        count = int(parts[2])
    except ValueError:
        count = 1
    if count <= 0:
        count = 1
    fields = parts[3].split("^")
    width = len(fields) // count
    if width < min_fields:
        return []
    return [fields[i * width:(i + 1) * width] for i in range(count)]


def parse_kis_frame(frame: str, today: date | None = None) -> list[TradeTick]:
    """체결가 프레임 -> 체결 목록. 심볼은 단축코드 그대로 (요청 표기 복원은 스트림이 한다).
    알 수 없는 TR·필드 부족은 빈 목록."""
    today = today or datetime.now(KST).date()
    ticks = []
    for record in _records(frame, TR_TRADE, _MIN_TRADE_FIELDS):
        try:
            ticks.append(_parse_record(record, today))
        except Exception:  # noqa: BLE001 - 레코드 단위 격리
            continue
    return ticks


def _parse_record(f: list[str], today: date) -> TradeTick:
    sign = f[3]
    change = _dec(f[4])
    # 부호 코드: 1 상한 2 상승 3 보합 4 하한 5 하락. 값이 부호 없이 오면 코드로 부호를 붙인다 (실측은 부호 포함)
    if change is not None and sign in ("4", "5") and change > 0:
        change = -change
    rate = _dec(f[5])
    return TradeTick(
        symbol=f[0],
        price=Decimal(f[2]),
        quantity=Decimal(f[12]),
        timestamp=_hhmmss(f[1], today),
        ask_price=_dec(f[10]),
        bid_price=_dec(f[11]),
        cumulative_volume=int(f[13]) if f[13].strip().isdigit() else None,
        change=change,
        change_rate=(rate / Decimal(100)).quantize(Decimal("0.000001")) if rate is not None else None,
    )


def parse_kis_order_book(frame: str, today: date | None = None) -> list[OrderBookTick]:
    """호가 프레임(H0STASP0) -> 호가창 목록. 0 호가는 빈 단계로 보고 뺀다"""
    today = today or datetime.now(KST).date()
    books = []
    for f in _records(frame, TR_ORDER_BOOK, _MIN_BOOK_FIELDS):
        try:
            def levels(price_from: int, qty_from: int) -> list[OrderBookLevel]:
                out = []
                for i in range(10):
                    price = _dec(f[price_from + i])
                    if price is None or price == 0:
                        continue
                    out.append(OrderBookLevel(price, _dec(f[qty_from + i]) or Decimal(0)))
                return out
            books.append(OrderBookTick(
                symbol=f[0],
                timestamp=_hhmmss(f[1], today),
                asks=levels(3, 23),
                bids=levels(13, 33),
                total_ask_quantity=_dec(f[43]),
                total_bid_quantity=_dec(f[44]),
            ))
        except Exception:  # noqa: BLE001
            continue
    return books


def parse_kis_order_events(frame: str, today: date | None = None) -> list[OrderEvent]:
    """주문 통보 프레임(복호화된 H0STCNI9/H0STCNI0 평문) -> 이벤트 목록"""
    today = today or datetime.now(KST).date()
    parts = frame.split("|", 2)
    tr_id = parts[1] if len(parts) > 1 else ""
    if tr_id not in (TR_ORDER_EVENTS_PAPER, TR_ORDER_EVENTS_LIVE):
        return []
    events = []
    for f in _records(frame, tr_id, _MIN_ORDER_FIELDS):
        try:
            events.append(_parse_order_event(f, today))
        except Exception:  # noqa: BLE001
            continue
    return events


def _parse_order_event(f: list[str], today: date) -> OrderEvent:
    rejected = f[12] == "1"
    filled = f[13] == "2"
    amend_kind = f[5]  # 0 정상, 1 정정, 2 취소
    if rejected:
        kind = OrderEventType.REJECTED
    elif filled:
        kind = OrderEventType.FILLED
    elif amend_kind == "2":
        kind = OrderEventType.CANCELED
    elif amend_kind == "1":
        kind = OrderEventType.MODIFIED
    else:
        kind = OrderEventType.ACCEPTED
    quantity = _dec(f[9]) if filled else _dec(f[16])
    price = _dec(f[10]) if filled else (_dec(f[25]) if len(f) > 25 else None)
    original = f[3].strip()
    if not original or not original.lstrip("0") or original == f[2]:
        original = None
    return OrderEvent(
        order_id=f[2].strip(),
        type=kind,
        timestamp=_hhmmss(f[11], today),
        symbol=f[8].strip(),
        side={"01": OrderSide.SELL, "02": OrderSide.BUY}.get(f[4]),
        quantity=quantity,
        price=price,
        original_order_id=original,
    )


def _aes():
    try:
        from cryptography.hazmat.primitives import padding
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    except ImportError as e:
        raise ImportError(
            "KIS 주문 통보 복호화에는 'cryptography' 패키지가 필요합니다: pip install 'hermetix[stream]'") from e
    return padding, Cipher, algorithms, modes


def kis_decrypt(base64_text: str, key: str, iv: str) -> str:
    """KIS 암호화 본문: AES-256-CBC, PKCS7, base64. key 32자·iv 16자는 구독 응답 output 에서 온다"""
    padding, Cipher, algorithms, modes = _aes()
    decryptor = Cipher(algorithms.AES(key.encode("utf-8")), modes.CBC(iv.encode("utf-8"))).decryptor()
    padded = decryptor.update(base64.b64decode(base64_text)) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    return (unpadder.update(padded) + unpadder.finalize()).decode("utf-8")


def kis_encrypt(plain: str, key: str, iv: str) -> str:
    """테스트·픽스처 생성용 - kis_decrypt 의 역"""
    padding, Cipher, algorithms, modes = _aes()
    padder = padding.PKCS7(128).padder()
    padded = padder.update(plain.encode("utf-8")) + padder.finalize()
    encryptor = Cipher(algorithms.AES(key.encode("utf-8")), modes.CBC(iv.encode("utf-8"))).encryptor()
    return base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode("ascii")


class KisMarketStream(ReconnectingWebSocket, MarketStream):

    def __init__(self, ws_url: str, custtype: str, approval_key: Callable[[], str],
                 hts_id: str = "", live: bool = False, usage=None):
        super().__init__("kis", usage=usage)
        self._ws_url = ws_url
        self._custtype = custtype
        self._approval_key = approval_key
        self._hts_id = hts_id
        self._tr_order_events = TR_ORDER_EVENTS_LIVE if live else TR_ORDER_EVENTS_PAPER
        self._lock = threading.Lock()
        self._trade_listeners: dict[str, list[TradeListener]] = {}       # 종목코드 -> 리스너
        self._book_listeners: dict[str, list[OrderBookListener]] = {}
        self._order_listeners: list[OrderEventListener] = []
        self._requested: dict[str, str] = {}                              # 종목코드 -> 구독 요청 표기
        self._cipher_keys: dict[str, tuple[str, str]] = {}                # 암호화 TR -> (key, iv), 구독 응답에서

    @property
    def is_connected(self) -> bool:
        return self.is_socket_open

    def uri(self) -> str:
        return self._ws_url

    def on_open(self) -> None:
        key = self._approval_key()
        with self._lock:
            trade_codes = list(self._trade_listeners)
            book_codes = list(self._book_listeners)
            has_orders = bool(self._order_listeners)
        for code in trade_codes:
            self.send(self._subscribe_message(key, TR_TRADE, code))
        for code in book_codes:
            self.send(self._subscribe_message(key, TR_ORDER_BOOK, code))
        if has_orders:
            self.send(self._subscribe_message(key, self._tr_order_events, self._hts_id))

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
            key = self._approval_key()
            for code in new_codes:
                self.send(self._subscribe_message(key, TR_TRADE, code))

    def subscribe_order_book(self, symbols: list[str], listener: OrderBookListener) -> None:
        new_codes = self._register(symbols, listener, self._book_listeners)
        if new_codes and self._usage is not None:
            self._usage.stream_subscribed(StreamChannel.ORDER_BOOK, len(new_codes))
        if new_codes and self.is_socket_open:
            key = self._approval_key()
            for code in new_codes:
                self.send(self._subscribe_message(key, TR_ORDER_BOOK, code))

    def subscribe_order_events(self, listener: OrderEventListener) -> None:
        if not self._hts_id.strip():
            raise ValueError("KIS 주문 통보 구독에는 HTS ID 가 필요합니다 (KisClient hts_id)")
        with self._lock:
            first = not self._order_listeners
            self._order_listeners.append(listener)
        if first and self._usage is not None:
            self._usage.stream_subscribed(StreamChannel.ORDER_EVENTS)
        if first and self.is_socket_open:
            self.send(self._subscribe_message(self._approval_key(), self._tr_order_events, self._hts_id))

    def on_message(self, text: str) -> None:
        if text.startswith("0|") or text.startswith("1|"):
            self._on_data_frame(text)
            return
        try:
            node = json.loads(text)
        except ValueError:
            return
        header = node.get("header") or {}
        tr_id = header.get("tr_id", "")
        if tr_id == "PINGPONG":
            self.send(text)
            return
        body = node.get("body")
        if not isinstance(body, dict):
            return
        rt_cd, msg_cd, msg, tr_key = body.get("rt_cd", ""), body.get("msg_cd", ""), body.get("msg1", ""), header.get("tr_key", "")
        output = body.get("output") or {}
        if isinstance(output, dict) and output.get("key") and output.get("iv"):
            self._cipher_keys[tr_id] = (output["key"], output["iv"])
        if rt_cd == "0" or msg_cd == "OPSP0000":
            logger.info("kis stream: subscribed %s %s (%s)", tr_id, "(hts)" if tr_id.startswith("H0STCNI") else tr_key, msg)
        elif msg_cd == "OPSP0002":
            logger.info("kis stream: already subscribed %s", tr_id)
        else:
            logger.warning("kis stream: %s %s rt_cd=%s msg_cd=%s %s", tr_id, tr_key, rt_cd, msg_cd, msg)

    def _on_data_frame(self, text: str) -> None:
        parts = text.split("|", 3)
        if len(parts) < 4:
            return
        tr_id = parts[1]
        if parts[0] == "1":
            keys = self._cipher_keys.get(tr_id)
            if keys is None:
                logger.warning("kis stream: 암호화 프레임(%s)인데 복호화 키가 없다 - 구독 응답 전 프레임?", tr_id)
                return
            try:
                body = kis_decrypt(parts[3], keys[0], keys[1])
            except Exception as e:  # noqa: BLE001
                logger.warning("kis stream: 복호화 실패(%s) - %s", tr_id, e)
                return
        else:
            body = parts[3]
        plain = f"0|{tr_id}|{parts[2]}|{body}"
        if tr_id == TR_TRADE:
            for tick in parse_kis_frame(plain):
                self._deliver(tick, self._trade_listeners, "리스너")
        elif tr_id == TR_ORDER_BOOK:
            for book in parse_kis_order_book(plain):
                self._deliver(book, self._book_listeners, "호가 리스너")
        elif tr_id in (TR_ORDER_EVENTS_PAPER, TR_ORDER_EVENTS_LIVE):
            with self._lock:
                listeners = list(self._order_listeners)
            for event in parse_kis_order_events(plain):
                if self._usage is not None:
                    self._usage.stream_message(StreamChannel.ORDER_EVENTS)
                for listener in listeners:
                    try:
                        listener(event)
                    except Exception:  # noqa: BLE001
                        logger.exception("kis stream: 주문 통보 리스너 오류 / %s", event.order_id)
        else:
            logger.debug("kis stream: unknown tr %s", tr_id)

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
                logger.exception("kis stream: %s 오류 / %s", label, symbol)

    def _subscribe_message(self, key: str, tr_id: str, tr_key: str, tr_type: str = "1") -> str:
        return json.dumps({
            "header": {"approval_key": key, "custtype": self._custtype, "tr_type": tr_type, "content-type": "utf-8"},
            "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}},
        })
