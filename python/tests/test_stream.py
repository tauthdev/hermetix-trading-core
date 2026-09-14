"""KIS·키움 웹소켓 스트림 - 로컬 가짜 서버로 구독·에코·틱 전달·재접속 흐름 검증 (Kotlin *MarketStreamTest 와 동일 시나리오)."""
import asyncio
import json
import queue
import threading

import pytest

websockets = pytest.importorskip("websockets")

from hermetix.brokers.kis_stream import KisMarketStream, kis_decrypt, kis_encrypt  # noqa: E402
from hermetix.brokers.kiwoom_stream import KiwoomMarketStream  # noqa: E402

pytest.importorskip("cryptography")

KEY = "zkkljxnqkyodprlmaksyyilhzjmxqjer"  # 32자 (실측 구독 응답과 같은 형식)
IV = "d82e2f422913e3b2"                   # 16자
# H0STASP0 본문 - 0 코드, 1 시각, 2 시간구분, 3-12 매도호가, 13-22 매수호가, 23-32 매도잔량, 33-42 매수잔량, 43 총매도잔량, 44 총매수잔량
KIS_BOOK_FIELDS = "^".join(["005930", "105530", "0"]
                           + [str(250500 + 500 * i) for i in range(10)] + [str(250000 - 500 * i) for i in range(10)]
                           + [str(1000 * (i + 1)) for i in range(10)] + [str(2000 * (i + 1)) for i in range(10)]
                           + ["55000", "65000", "0", "0"])
KIS_ORDER_ACCEPTED = "HTSUSER^50199202^0000012345^0000000000^02^0^00^0^005930^0^0^105530^0^1^1^00950^3^홍길동^0^^N^^^^삼성전자^250000"
KIS_ORDER_FILLED = "HTSUSER^50199202^0000012345^0000000000^02^0^00^0^005930^3^250000^105531^0^2^2^00950^3^홍길동^0^^N^^^^삼성전자^250000"
_book_values = {"21": "105530", "121": "55000", "125": "65000"}
for _i in range(10):
    _book_values[str(41 + _i)] = f"-{250500 + 500 * _i}"; _book_values[str(61 + _i)] = str(1000 * (_i + 1))
    _book_values[str(51 + _i)] = f"-{250000 - 500 * _i}"; _book_values[str(71 + _i)] = str(2000 * (_i + 1))
KIWOOM_BOOK = json.dumps({"data": [{"values": _book_values, "type": "0D", "name": "주식호가잔량", "item": "A005930"}], "trnm": "REAL"})
KIWOOM_ORDER = json.dumps({"data": [{"values": {
    "9203": "0000012345", "904": "0000000000", "9001": "A005930", "913": "체결", "905": "+매수", "907": "2", "900": "1",
    "901": "+250000", "902": "0", "910": "+250000", "911": "1", "908": "105531", "919": ""},
    "type": "00", "name": "주문체결", "item": ""}], "trnm": "REAL"})

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

def kis_stream(server, hts_id: str = "HTSUSER") -> KisMarketStream:
    return KisMarketStream(f"ws://127.0.0.1:{server.port}/", "P", lambda: "APPROVAL-KEY", hts_id=hts_id)


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


def test_kis_order_book_subscribe_and_frame(server):
    books: queue.Queue = queue.Queue()
    stream = kis_stream(server)
    try:
        stream.subscribe_order_book(["KRX:005930"], books.put)
        stream.connect()
        conn = server.take()
        assert json.loads(conn.take())["body"]["input"] == {"tr_id": "H0STASP0", "tr_key": "005930"}
        conn.send("0|H0STASP0|001|" + KIS_BOOK_FIELDS)
        book = books.get(timeout=5)
        assert book.symbol == "KRX:005930"
        assert len(book.asks) == 10 and len(book.bids) == 10
        assert (str(book.best_ask.price), str(book.best_ask.quantity)) == ("250500", "1000")
        assert (str(book.best_bid.price), str(book.best_bid.quantity)) == ("250000", "2000")
        assert str(book.asks[9].price) == "255000"
        assert str(book.total_ask_quantity) == "55000" and str(book.total_bid_quantity) == "65000"
    finally:
        stream.close()


def test_kis_order_events_decrypt_with_key_iv_from_subscribe_response(server):
    events: queue.Queue = queue.Queue()
    stream = kis_stream(server)
    try:
        stream.subscribe_order_events(events.put)
        stream.connect()
        conn = server.take()
        subscribe = json.loads(conn.take())
        assert subscribe["body"]["input"] == {"tr_id": "H0STCNI9", "tr_key": "HTSUSER"}  # 모의

        conn.send(json.dumps({"header": {"tr_id": "H0STCNI9", "tr_key": "HTSUSER", "encrypt": "Y"},
                              "body": {"rt_cd": "0", "msg_cd": "OPSP0000", "msg1": "SUBSCRIBE SUCCESS", "output": {"iv": IV, "key": KEY}}}))
        conn.send("1|H0STCNI9|001|" + kis_encrypt(KIS_ORDER_ACCEPTED, KEY, IV))
        conn.send("1|H0STCNI9|001|" + kis_encrypt(KIS_ORDER_FILLED, KEY, IV))

        e1 = events.get(timeout=5)
        assert e1.type.name == "ACCEPTED" and e1.order_id == "0000012345" and e1.order_id_matches("12345")
        assert e1.symbol == "005930" and e1.side.name == "BUY" and e1.quantity == 3 and e1.price == 250000
        e2 = events.get(timeout=5)
        assert e2.type.name == "FILLED" and e2.quantity == 3 and e2.price == 250000
    finally:
        stream.close()


def test_kis_order_events_require_hts_id(server):
    stream = kis_stream(server, hts_id="")
    with pytest.raises(ValueError, match="hts_id"):
        stream.subscribe_order_events(lambda e: None)


def test_kis_aes_roundtrip():
    plain = "005930^105530^250000"
    assert kis_decrypt(kis_encrypt(plain, KEY, IV), KEY, IV) == plain


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


def test_kiwoom_registers_order_book_by_code_and_order_events_with_empty_item(server):
    books: queue.Queue = queue.Queue()
    events: queue.Queue = queue.Queue()
    stream = kiwoom_stream(server)
    try:
        stream.subscribe_order_book(["KRX:005930"], books.put)
        stream.subscribe_order_events(events.put)
        stream.connect()
        conn = server.take()
        conn.take()  # LOGIN
        conn.send('{"trnm":"LOGIN","return_code":0}')
        regs = [json.loads(conn.take()) for _ in range(2)]
        by_type = {r["data"][0]["type"][0]: r["data"][0]["item"] for r in regs}
        assert by_type == {"0D": ["005930"], "00": [""]}

        conn.send(KIWOOM_BOOK)
        book = books.get(timeout=5)
        assert book.symbol == "KRX:005930"
        assert (str(book.best_ask.price), str(book.best_ask.quantity)) == ("250500", "1000")
        assert str(book.best_bid.price) == "250000" and str(book.bids[1].price) == "249500"
        assert str(book.total_ask_quantity) == "55000"

        conn.send(KIWOOM_ORDER)
        e = events.get(timeout=5)
        assert e.type.name == "FILLED" and e.order_id == "0000012345" and e.symbol == "005930"
        assert e.side.name == "BUY" and e.quantity == 1 and e.price == 250000 and e.remaining_quantity == 0
    finally:
        stream.close()
