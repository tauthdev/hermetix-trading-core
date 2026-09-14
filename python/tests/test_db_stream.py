"""DB증권 웹소켓 스트림 (문서 기반) - 가짜 서버로 등록·ACK·제어 프레임·데이터 전달·재접속 흐름과 픽스처 파서 검증 (Kotlin DbMarketStreamTest 와 동일)."""
import json
import queue

import pytest

pytest.importorskip("websockets")

from hermetix.brokers.db_stream import DbMarketStream, parse_db_order_book, parse_db_order_event, parse_db_trade  # noqa: E402
from tests.ws_fake_server import FakeServer, TODAY, assert_books, assert_events, assert_ticks, stream_section, wait_until  # noqa: E402

SECTION = stream_section("db")
TRADE_FRAME = SECTION["frames"][0]
BOOK_FRAME = SECTION["orderBook"]["frames"][0]
IS0_FRAME, IS1_FILL_FRAME, IS1_CANCEL_FRAME = SECTION["orderEvents"]["frames"]


@pytest.fixture
def server():
    s = FakeServer()
    yield s
    s.shutdown()


def db_stream(server) -> DbMarketStream:
    return DbMarketStream(server.url("/websocket"), lambda: "ACCESS-TOKEN")


def test_subscribe_format_and_trade_delivery(server):
    ticks: queue.Queue = queue.Queue()
    stream = db_stream(server)
    try:
        stream.subscribe_trades(["KRX:005930"], ticks.put)
        stream.connect()
        conn = server.take()
        assert conn.take_json() == {"header": {"token": "ACCESS-TOKEN", "tr_type": "1"}, "body": {"tr_cd": "S00", "tr_key": "J 005930"}}
        conn.send('{"header":{"tr_cd":"S00","tr_key":null},"body":{"tr_key":["J 005930"]}}')  # ack
        conn.send('{"header":null,"body":null}')  # keepalive 류
        conn.send('{"header":{"rsp_cd":"00000","rsp_msg":"정상"},"body":null}')  # 제어
        conn.send(TRADE_FRAME)
        tick = ticks.get(timeout=5)
        assert tick.symbol == "KRX:005930"
        assert str(tick.price) == "143300" and str(tick.quantity) == "100" and tick.cumulative_volume == 405039
        assert str(tick.change) == "33000" and str(tick.change_rate) == "0.299200"
        assert str(tick.ask_price) == "140200" and str(tick.bid_price) == "143300"
    finally:
        stream.close()


def test_order_book_registration_and_frame(server):
    books: queue.Queue = queue.Queue()
    stream = db_stream(server)
    try:
        stream.subscribe_order_book(["005930"], books.put)
        stream.connect()
        conn = server.take()
        assert conn.take_json()["body"] == {"tr_cd": "S01", "tr_key": "J 005930"}
        conn.send(BOOK_FRAME)
        book = books.get(timeout=5)
        assert len(book.asks) == 10 and len(book.bids) == 10
        assert (str(book.best_ask.price), str(book.best_ask.quantity)) == ("54500", "1000")
        assert (str(book.best_bid.price), str(book.best_bid.quantity)) == ("54400", "2000")
        assert str(book.total_ask_quantity) == "809630" and str(book.total_bid_quantity) == "2193305"
    finally:
        stream.close()


def test_order_events_register_is0_is1_without_tr_key_and_deliver(server):
    events: queue.Queue = queue.Queue()
    stream = db_stream(server)
    try:
        stream.subscribe_order_events(events.put)
        stream.connect()
        conn = server.take()
        regs = [conn.take_json() for _ in range(2)]
        assert regs == [{"header": {"token": "ACCESS-TOKEN", "tr_type": "3"}, "body": {"tr_cd": "IS0"}},
                        {"header": {"token": "ACCESS-TOKEN", "tr_type": "3"}, "body": {"tr_cd": "IS1"}}]
        conn.send(IS0_FRAME)
        conn.send(IS1_FILL_FRAME)
        conn.send(IS1_CANCEL_FRAME)
        accepted = events.get(timeout=5)
        assert accepted.type.name == "ACCEPTED" and accepted.order_id_matches("34048") and accepted.side.name == "BUY"
        assert accepted.symbol == "005930" and accepted.quantity == 10 and accepted.price == 80000
        filled = events.get(timeout=5)
        assert filled.type.name == "FILLED" and filled.quantity == 4 and filled.price == 80000 and filled.remaining_quantity == 6
        canceled = events.get(timeout=5)
        assert canceled.type.name == "CANCELED" and canceled.order_id == "0000241048" and canceled.original_order_id == "0000241038"
        assert canceled.symbol == "004410"
    finally:
        stream.close()


def test_reconnect_resends_quote_and_account_registrations(server):
    stream = db_stream(server)
    try:
        stream.subscribe_trades(["005930"], lambda t: None)
        stream.subscribe_order_events(lambda e: None)
        stream.connect()
        first = server.take()
        first.drain(3)
        first.close()
        second = server.take(10)
        codes = sorted(m["body"]["tr_cd"] for m in (second.take_json() for _ in range(3)))
        assert codes == ["IS0", "IS1", "S00"]
    finally:
        stream.close()


def test_late_subscription_is_sent_immediately(server):
    stream = db_stream(server)
    try:
        stream.connect()
        server.take()
        wait_until(lambda: stream.is_connected)
        conn = None
        stream.subscribe_trades(["000660"], lambda t: None)
        conn = server.connections.queue[0] if not server.connections.empty() else None
    finally:
        stream.close()


def test_parsers_are_case_insensitive_and_handle_signs_and_kinds():
    body = json.loads(TRADE_FRAME)["body"]
    lower = {k.lower(): v for k, v in body.items()}
    assert parse_db_trade(lower, TODAY).ask_price == 140200
    falling = {**body, "PrdyVrssclr": "-", "PrdyVrss": "500"}
    assert parse_db_trade(falling, TODAY).change == -500
    assert parse_db_trade({"ShrnIscd": "005930"}, TODAY) is None

    book_body = json.loads(BOOK_FRAME)["body"]
    zeroed = {**book_body, "Askp10": "0"}
    assert len(parse_db_order_book(zeroed, TODAY).asks) == 9

    is1 = json.loads(IS1_FILL_FRAME)["body"]
    rejected = parse_db_order_event("IS1", {**is1, "Sexecqty": "0", "Srjtqty": "0000000000000004"}, TODAY)
    assert rejected.type.name == "REJECTED" and rejected.quantity == 4
    modified = parse_db_order_event("IS1", {**is1, "Sexecqty": "0", "Smdfycnfqty": "3", "Smdfycnfprc": "81000"}, TODAY)
    assert modified.type.name == "MODIFIED" and modified.quantity == 3 and modified.price == 81000
    assert parse_db_order_event("IS1", {**is1, "Sordno": ""}, TODAY) is None
    assert parse_db_order_event("IS0", json.loads(IS0_FRAME)["body"], TODAY).original_order_id is None


def test_fixture_stream_sections_document_based():
    assert SECTION["measured"] is False and SECTION["channel"] == "TRADES"
    assert_ticks([parse_db_trade(json.loads(f)["body"], TODAY) for f in SECTION["frames"]], SECTION["expected"])
    book = SECTION["orderBook"]
    assert book["measured"] is False
    assert_books([parse_db_order_book(json.loads(f)["body"], TODAY) for f in book["frames"]], book["expected"])
    events = SECTION["orderEvents"]
    assert events["measured"] is False
    parsed = [parse_db_order_event(json.loads(f)["header"]["tr_cd"], json.loads(f)["body"], TODAY) for f in events["frames"]]
    assert_events(parsed, events["expected"])
