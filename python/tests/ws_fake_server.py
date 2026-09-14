"""브로커 스트림 테스트 공용 가짜 웹소켓 서버 (websockets.serve 기반). test_stream.py 의 것과 같은 모양이며
핸드셰이크 헤더도 기록한다 (토스 Authorization 검증용)."""
from __future__ import annotations

import asyncio
import json
import queue
import threading
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import websockets

FIXTURES = Path(__file__).resolve().parents[2] / "conformance" / "fixtures"
KST = ZoneInfo("Asia/Seoul")
TODAY = date(2026, 9, 14)


class ServerConnection:
    """서버 쪽에서 본 연결 하나 - 받은 메시지 큐, 핸드셰이크 헤더, 보낼 수 있는 소켓."""

    def __init__(self, ws, loop):
        self._ws = ws
        self._loop = loop
        self.received: queue.Queue[str] = queue.Queue()
        request = getattr(ws, "request", None)
        headers = getattr(request, "headers", None) if request is not None else getattr(ws, "request_headers", None)
        self.headers = {k.lower(): v for k, v in (headers.items() if headers is not None else [])}

    def send(self, text: str) -> None:
        asyncio.run_coroutine_threadsafe(self._ws.send(text), self._loop).result(5)

    def close(self) -> None:
        asyncio.run_coroutine_threadsafe(self._ws.close(1000, "server going away"), self._loop).result(5)

    def take(self, seconds: float = 5) -> str:
        return self.received.get(timeout=seconds)

    def take_json(self, seconds: float = 5):
        return json.loads(self.take(seconds))

    def drain(self, count: int, seconds: float = 5) -> list[str]:
        return [self.take(seconds) for _ in range(count)]


class FakeServer:

    def __init__(self):
        self.connections: queue.Queue[ServerConnection] = queue.Queue()
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self.port = 0
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        self._ready.wait(5)

    def _run(self):
        asyncio.set_event_loop(self._loop)

        async def handler(ws):
            conn = ServerConnection(ws, self._loop)
            self.connections.put(conn)
            try:
                async for message in ws:
                    conn.received.put(message)
            except Exception:  # noqa: BLE001
                pass

        async def main():
            self._server = await websockets.serve(handler, "127.0.0.1", 0)
            self.port = self._server.sockets[0].getsockname()[1]
            self._ready.set()
            await asyncio.Future()

        try:
            self._loop.run_until_complete(main())
        except Exception:  # noqa: BLE001
            pass

    def url(self, path: str = "/") -> str:
        return f"ws://127.0.0.1:{self.port}{path}"

    def take(self, seconds: float = 5) -> ServerConnection:
        return self.connections.get(timeout=seconds)

    def shutdown(self):
        async def close():
            self._server.close()
            await self._server.wait_closed()
        asyncio.run_coroutine_threadsafe(close(), self._loop).result(5)
        self._loop.call_soon_threadsafe(self._loop.stop)


def wait_until(predicate, seconds: float = 5):
    deadline = time.monotonic() + seconds
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert predicate()


def stream_section(broker: str) -> dict:
    fx = json.loads((FIXTURES / f"{broker}.json").read_text())
    assert "stream" in fx, f"{broker} 픽스처에 stream 섹션이 없다"
    return fx["stream"]


def _time_matches(dt, expected: str) -> bool:
    local = dt.astimezone(KST)
    return local.strftime("%H:%M:%S") == expected or local.strftime("%H:%M") == expected


def _dec_or_none(value):
    return None if value is None else Decimal(value)


def assert_ticks(ticks, expected):
    assert len(ticks) == len(expected)
    for tick, e in zip(ticks, expected):
        assert tick.symbol == e["symbol"]
        assert tick.price == Decimal(e["price"])
        assert tick.quantity == Decimal(e["quantity"])
        assert tick.ask_price == _dec_or_none(e["askPrice"])
        assert tick.bid_price == _dec_or_none(e["bidPrice"])
        assert tick.cumulative_volume == e["cumulativeVolume"]
        assert tick.change == _dec_or_none(e["change"])
        assert tick.change_rate == _dec_or_none(e["changeRate"])
        assert _time_matches(tick.timestamp, e["time"])


def assert_books(books, expected):
    assert len(books) == len(expected)
    for book, e in zip(books, expected):
        assert book.symbol == e["symbol"]
        assert _time_matches(book.timestamp, e["time"])
        assert [(str(l.price), str(l.quantity)) for l in book.asks] == [(l["price"], l["quantity"]) for l in e["asks"]]
        assert [(str(l.price), str(l.quantity)) for l in book.bids] == [(l["price"], l["quantity"]) for l in e["bids"]]
        assert book.total_ask_quantity == _dec_or_none(e["totalAskQuantity"])
        assert book.total_bid_quantity == _dec_or_none(e["totalBidQuantity"])


def assert_events(events, expected):
    assert len(events) == len(expected)
    for ev, e in zip(events, expected):
        assert ev.order_id == e["orderId"]
        assert ev.type.name == e["type"]
        assert _time_matches(ev.timestamp, e["time"])
        assert ev.symbol == e["symbol"]
        assert ev.side.name == e["side"]
        assert ev.quantity == Decimal(e["quantity"])
        assert ev.price == Decimal(e["price"])
        if "remainingQuantity" in e:
            assert ev.remaining_quantity == Decimal(e["remainingQuantity"])
        assert ev.original_order_id == e.get("originalOrderId")
