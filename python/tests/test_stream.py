"""KIS·키움 웹소켓 스트림 - 로컬 가짜 서버로 구독·에코·틱 전달·재접속 흐름 검증 (Kotlin *MarketStreamTest 와 동일 시나리오)."""
import asyncio
import json
import queue
import threading

import pytest

websockets = pytest.importorskip("websockets")

from hermetix.brokers.kis_stream import KisMarketStream  # noqa: E402
from hermetix.brokers.kiwoom_stream import KiwoomMarketStream  # noqa: E402

KIS_FIELDS = "005930^093012^71500^5^300^-0.42^71480^71800^71900^71300^71500^71400^15^1234567^0^0^0^0^0^0"
KIWOOM_REAL = json.dumps({"data": [{"type": "0B", "name": "주식체결", "item": "A005930", "values": {
    "20": "093012", "10": "-71500", "11": "-300", "12": "-0.42", "27": "+71500", "28": "+71400", "15": "-15", "13": "1234567"}}],
    "trnm": "REAL"})


class ServerConnection:
    """서버 쪽에서 본 연결 하나 - 받은 메시지 큐와 보낼 수 있는 소켓."""

    def __init__(self, ws, loop):
        self._ws = ws
        self._loop = loop
        self.received: queue.Queue[str] = queue.Queue()

    def send(self, text: str) -> None:
        asyncio.run_coroutine_threadsafe(self._ws.send(text), self._loop).result(5)

    def close(self) -> None:
        asyncio.run_coroutine_threadsafe(self._ws.close(1000, "server going away"), self._loop).result(5)

    def take(self, seconds: float = 5) -> str:
        return self.received.get(timeout=seconds)


class FakeServer:
    """websockets.serve 기반 가짜 브로커 - 연결마다 ServerConnection 을 큐에 넣는다."""

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

    def take(self, seconds: float = 5) -> ServerConnection:
        return self.connections.get(timeout=seconds)

    def shutdown(self):
        async def close():
            self._server.close()
            await self._server.wait_closed()
        asyncio.run_coroutine_threadsafe(close(), self._loop).result(5)
        self._loop.call_soon_threadsafe(self._loop.stop)


@pytest.fixture
def server():
    s = FakeServer()
    yield s
    s.shutdown()


def wait_until(predicate, seconds: float = 5):
    import time
    deadline = time.monotonic() + seconds
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert predicate()


# ------------------------------------------------------------------ KIS

def kis_stream(server) -> KisMarketStream:
    return KisMarketStream(f"ws://127.0.0.1:{server.port}/", "P", lambda: "APPROVAL-KEY")


def test_kis_subscribes_with_approval_key_and_delivers_tick_in_requested_notation(server):
    ticks: queue.Queue = queue.Queue()
    stream = kis_stream(server)
    try:
        stream.subscribe_trades(["KRX:005930"], ticks.put)
        stream.connect()
        conn = server.take()
        subscribe = json.loads(conn.take())
        assert subscribe["header"]["approval_key"] == "APPROVAL-KEY"
        assert subscribe["header"]["tr_type"] == "1"
        assert subscribe["body"]["input"] == {"tr_id": "H0STCNT0", "tr_key": "005930"}

        conn.send('{"header":{"tr_id":"H0STCNT0","tr_key":"005930","encrypt":"N"},"body":{"rt_cd":"0","msg_cd":"OPSP0000","msg1":"SUBSCRIBE SUCCESS"}}')
        conn.send("0|H0STCNT0|001|" + KIS_FIELDS)
        tick = ticks.get(timeout=5)
        assert tick.symbol == "KRX:005930"
        assert str(tick.price) == "71500" and str(tick.quantity) == "15" and tick.cumulative_volume == 1234567
        assert stream.is_connected
    finally:
        stream.close()


def test_kis_echoes_pingpong(server):
    stream = kis_stream(server)
    try:
        stream.connect()
        conn = server.take()
        ping = '{"header":{"tr_id":"PINGPONG","datetime":"20260914093000"}}'
        conn.send(ping)
        assert conn.take() == ping
    finally:
        stream.close()


def test_kis_reconnects_and_resubscribes_after_server_close(server):
    stream = kis_stream(server)
    try:
        stream.subscribe_trades(["005930"], lambda t: None)
        stream.connect()
        first = server.take()
        first.take()  # 첫 구독
        first.close()
        second = server.take(10)
        assert json.loads(second.take())["body"]["input"]["tr_key"] == "005930"
    finally:
        stream.close()


def test_kis_late_subscription_is_sent_immediately(server):
    stream = kis_stream(server)
    try:
        stream.connect()
        conn = server.take()
        wait_until(lambda: stream.is_connected)
        stream.subscribe_trades(["000660"], lambda t: None)
        assert json.loads(conn.take())["body"]["input"]["tr_key"] == "000660"
    finally:
        stream.close()


# ------------------------------------------------------------------ 키움

def kiwoom_stream(server) -> KiwoomMarketStream:
    return KiwoomMarketStream(f"ws://127.0.0.1:{server.port}/api/dostk/websocket", lambda: "ACCESS-TOKEN")


def test_kiwoom_login_register_real_flow(server):
    ticks: queue.Queue = queue.Queue()
    stream = kiwoom_stream(server)
    try:
        stream.subscribe_trades(["KRX:005930"], ticks.put)
        stream.connect()
        conn = server.take()
        login = json.loads(conn.take())
        assert login == {"trnm": "LOGIN", "token": "ACCESS-TOKEN"}
        assert not stream.is_connected  # 로그인 응답 전

        conn.send('{"trnm":"LOGIN","return_code":0,"return_msg":"","sor_yn":"Y"}')
        reg = json.loads(conn.take())
        assert reg["trnm"] == "REG" and reg["data"][0] == {"item": ["005930"], "type": ["0B"]}
        conn.send('{"trnm":"REG","return_code":0,"return_msg":""}')
        conn.send(KIWOOM_REAL)
        tick = ticks.get(timeout=5)
        assert tick.symbol == "KRX:005930"
        assert str(tick.price) == "71500" and str(tick.quantity) == "15"
        assert stream.is_connected
    finally:
        stream.close()


def test_kiwoom_echoes_ping(server):
    stream = kiwoom_stream(server)
    try:
        stream.connect()
        conn = server.take()
        conn.take()  # LOGIN
        conn.send('{"trnm":"PING"}')
        assert conn.take() == '{"trnm":"PING"}'
    finally:
        stream.close()


def test_kiwoom_reconnects_relogins_and_reregisters(server):
    stream = kiwoom_stream(server)
    try:
        stream.subscribe_trades(["005930"], lambda t: None)
        stream.connect()
        first = server.take()
        first.take()  # LOGIN
        first.close()
        second = server.take(10)
        assert json.loads(second.take())["trnm"] == "LOGIN"
        second.send('{"trnm":"LOGIN","return_code":0}')
        assert json.loads(second.take())["trnm"] == "REG"
    finally:
        stream.close()
