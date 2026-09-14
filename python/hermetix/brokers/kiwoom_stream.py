"""키움 REST API 실시간 체결 스트림 (실시간 타입 0B 주식체결). 2026-09-14 모의투자 서버(mockapi) 장중 실측 통과 (Kotlin 레퍼런스).

프로토콜:
- 접속: 모의 wss://mockapi.kiwoom.com:10000/api/dostk/websocket, 실전 wss://api.kiwoom.com:10000/...
- 로그인: 접속 직후 {"trnm":"LOGIN","token":<접근토큰>} -> {"trnm":"LOGIN","return_code":0,"sor_yn":"Y"}. REST 토큰을 그대로 쓴다
- 등록: {"trnm":"REG","grp_no":"1","refresh":"1","data":[{"item":[코드...],"type":["0B"]}]} -> {"trnm":"REG","return_code":0}
- 데이터: {"data":[{"type":"0B","name":"주식체결","item":"005930","values":{"20":체결시각,"10":현재가,...}}],"trnm":"REAL"}
  (실측: data 키가 trnm 보다 앞에 온다 - 키 순서 무관)
- {"trnm":"PING"} 은 받은 그대로 되돌려 보낸다

values 의 FID: 20 체결시각 HHMMSS, 10 현재가, 11 전일대비, 12 등락율(%), 27 최우선매도호가, 28 최우선매수호가,
15 거래량(+매수/-매도 체결), 13 누적거래량. REST 와 같이 가격·호가·체결량에 등락 부호가 붙으므로 절대값으로 파싱한다.
실측 프레임은 conformance/fixtures/kiwoom.json#stream.
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

TYPE_TRADE = "0B"


def _signed(values: dict, fid: str) -> Decimal | None:
    text = str(values.get(fid, "")).strip().lstrip("+")
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def parse_kiwoom_real(obj: dict, today: date | None = None) -> list[TradeTick]:
    """REAL 프레임 -> 체결 목록 (0B 만). 심볼은 종목코드 그대로 (요청 표기 복원은 스트림이 한다)."""
    today = today or datetime.now(KST).date()
    ticks = []
    for item in obj.get("data") or []:
        if not isinstance(item, dict) or item.get("type") != TYPE_TRADE:
            continue
        try:
            ticks.append(_parse_item(item, today))
        except Exception:  # noqa: BLE001 - 항목 단위 격리
            continue
    return ticks


def _parse_item(item: dict, today: date) -> TradeTick:
    v = item.get("values") or {}
    t = str(v.get("20", "")).strip().rjust(6, "0")
    price = _signed(v, "10")
    if price is None:
        raise ValueError("no price")
    quantity = _signed(v, "15")
    cum = _signed(v, "13")
    rate = _signed(v, "12")
    ask, bid = _signed(v, "27"), _signed(v, "28")
    return TradeTick(
        # 종목코드는 REST 와 같이 A 프리픽스가 붙어 올 수 있다 ("A005930")
        symbol=str(item.get("item", "")).strip().lstrip("A"),
        price=abs(price),
        quantity=abs(quantity) if quantity is not None else Decimal(0),
        timestamp=datetime.combine(today, dtime(int(t[0:2]), int(t[2:4]), int(t[4:6])), tzinfo=KST),
        ask_price=abs(ask) if ask is not None else None,
        bid_price=abs(bid) if bid is not None else None,
        cumulative_volume=int(abs(cum)) if cum is not None else None,
        change=_signed(v, "11"),
        change_rate=(rate / Decimal(100)).quantize(Decimal("0.000001")) if rate is not None else None,
    )


class KiwoomMarketStream(ReconnectingWebSocket, MarketStream):

    def __init__(self, ws_url: str, token: Callable[[], str]):
        super().__init__("kiwoom")
        self._ws_url = ws_url
        self._token = token
        self._lock = threading.Lock()
        self._listeners: dict[str, list[TradeListener]] = {}
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
        if new_codes and self.is_connected:
            self.send(self._register_message(new_codes))

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
                with self._lock:
                    codes = list(self._listeners)
                if codes:
                    self.send(self._register_message(codes))
            else:
                logger.error("kiwoom stream: 로그인 실패 return_code=%s %s", code, node.get("return_msg", ""))
        elif trnm == "REG":
            code = node.get("return_code", -1)
            if code == 0:
                with self._lock:
                    codes = list(self._listeners)
                logger.info("kiwoom stream: registered %s", codes)
            else:
                logger.warning("kiwoom stream: 등록 실패 return_code=%s %s", code, node.get("return_msg", ""))
        elif trnm == "REAL":
            for tick in parse_kiwoom_real(node):
                self._deliver(tick)
        else:
            logger.debug("kiwoom stream: %s", text[:200])

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
                logger.exception("kiwoom stream: 리스너 오류 / %s", symbol)

    @staticmethod
    def _register_message(codes: list[str]) -> str:
        return json.dumps({"trnm": "REG", "grp_no": "1", "refresh": "1",
                           "data": [{"item": codes, "type": [TYPE_TRADE]}]})
