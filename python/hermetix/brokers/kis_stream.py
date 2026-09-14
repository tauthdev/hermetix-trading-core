"""KIS 실시간 체결가 스트림 (TR H0STCNT0). 2026-09-14 모의투자 서버(ops...:31000) 장중 실측 통과 (Kotlin 레퍼런스).

프로토콜:
- 접속: 모의 ws://ops.koreainvestment.com:31000, 실전 :21000. TLS 없음
- 인증: REST POST /oauth2/Approval 로 받은 approval_key 를 구독 메시지 헤더에 싣는다 (매 접속마다 재발급 가능)
- 구독: {"header":{"approval_key","custtype","tr_type":"1","content-type":"utf-8"},"body":{"input":{"tr_id":"H0STCNT0","tr_key":"005930"}}}
- 데이터 프레임: 0|H0STCNT0|<건수>|<필드^필드^...> - 암호화 플래그(0/1)·TR·레코드 수·^ 구분 본문.
  레코드가 여러 건이면 본문에 이어 붙는다 (필드 폭 = 전체 필드 수 / 건수, 실측 폭 47)
- 제어 프레임(JSON): 구독 결과(body.rt_cd/msg_cd), PINGPONG(그대로 되돌려 보내야 연결 유지)

필드 순서(0부터): 0 단축코드, 1 체결시각 HHMMSS, 2 현재가, 3 전일대비부호, 4 전일대비(부호 포함), 5 전일대비율(%),
6 가중평균가, 7 시가, 8 고가, 9 저가, 10 매도호가1, 11 매수호가1, 12 체결거래량, 13 누적거래량, ...
실측 프레임은 conformance/fixtures/kis.json#stream.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import date, datetime, time as dtime
from decimal import Decimal
from typing import Callable

from ..broker import KST, MarketStream, TradeListener
from ..models import TradeTick, symbol_code
from ..stream import ReconnectingWebSocket

logger = logging.getLogger("hermetix")

TR_TRADE = "H0STCNT0"
_MIN_FIELDS = 14


def _dec(text: str) -> Decimal | None:
    try:
        return Decimal(text.strip()) if text.strip() else None
    except Exception:
        return None


def _hhmmss(text: str, today: date) -> datetime:
    t = text.strip().rjust(6, "0")
    return datetime.combine(today, dtime(int(t[0:2]), int(t[2:4]), int(t[4:6])), tzinfo=KST)


def parse_kis_frame(frame: str, today: date | None = None) -> list[TradeTick]:
    """데이터 프레임 -> 체결 목록. 심볼은 단축코드 그대로 (요청 표기 복원은 스트림이 한다).
    알 수 없는 TR·필드 부족은 빈 목록."""
    today = today or datetime.now(KST).date()
    parts = frame.split("|", 3)
    if len(parts) < 4 or parts[1] != TR_TRADE:
        return []
    try:
        count = int(parts[2])
    except ValueError:
        count = 1
    if count <= 0:
        count = 1
    fields = parts[3].split("^")
    width = len(fields) // count
    if width < _MIN_FIELDS:
        return []
    ticks = []
    for i in range(count):
        record = fields[i * width:(i + 1) * width]
        try:
            ticks.append(_parse_record(record, today))
        except Exception:  # noqa: BLE001 - 레코드 단위 격리
            continue
    return ticks


def _parse_record(f: list[str], today: date) -> TradeTick:
    sign = f[3]
    change = _dec(f[4])
    # 부호 코드: 1 상한 2 상승 3 보합 4 하한 5 하락. 값이 부호 없이 오면 코드로 부호를 붙인다
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


class KisMarketStream(ReconnectingWebSocket, MarketStream):

    def __init__(self, ws_url: str, custtype: str, approval_key: Callable[[], str]):
        super().__init__("kis")
        self._ws_url = ws_url
        self._custtype = custtype
        self._approval_key = approval_key
        self._lock = threading.Lock()
        self._listeners: dict[str, list[TradeListener]] = {}   # 종목코드 -> 리스너
        self._requested: dict[str, str] = {}                   # 종목코드 -> 구독 요청 표기

    @property
    def is_connected(self) -> bool:
        return self.is_socket_open

    def uri(self) -> str:
        return self._ws_url

    def on_open(self) -> None:
        key = self._approval_key()
        with self._lock:
            codes = list(self._listeners)
        for code in codes:
            self.send(self._subscribe_message(key, code))

    def subscribe_trades(self, symbols: list[str], listener: TradeListener) -> None:
        new_codes = []
        with self._lock:
            for symbol in symbols:
                code = symbol_code(symbol)
                self._requested.setdefault(code, symbol)
                if code not in self._listeners:
                    self._listeners[code] = []
                    new_codes.append(code)
                self._listeners[code].append(listener)
        if new_codes and self.is_socket_open:
            key = self._approval_key()
            for code in new_codes:
                self.send(self._subscribe_message(key, code))

    def on_message(self, text: str) -> None:
        if text.startswith("0|") or text.startswith("1|"):
            for tick in parse_kis_frame(text):
                self._deliver(tick)
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
        if rt_cd == "0" or msg_cd == "OPSP0000":
            logger.info("kis stream: subscribed %s %s (%s)", tr_id, tr_key, msg)
        elif msg_cd == "OPSP0002":
            logger.info("kis stream: already subscribed %s %s", tr_id, tr_key)
        else:
            logger.warning("kis stream: %s %s rt_cd=%s msg_cd=%s %s", tr_id, tr_key, rt_cd, msg_cd, msg)

    def _deliver(self, tick: TradeTick) -> None:
        code = tick.symbol
        with self._lock:
            symbol = self._requested.get(code, code)
            listeners = list(self._listeners.get(code, ()))
        normalized = tick if symbol == code else TradeTick(**{**tick.__dict__, "symbol": symbol})
        for listener in listeners:
            try:
                listener(normalized)
            except Exception:  # noqa: BLE001
                logger.exception("kis stream: 리스너 오류 / %s", symbol)

    def _subscribe_message(self, key: str, code: str, tr_type: str = "1") -> str:
        return json.dumps({
            "header": {"approval_key": key, "custtype": self._custtype, "tr_type": tr_type, "content-type": "utf-8"},
            "body": {"input": {"tr_id": TR_TRADE, "tr_key": code}},
        })
