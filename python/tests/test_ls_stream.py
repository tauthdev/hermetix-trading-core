"""LS증권 웹소켓 스트림 (문서 기반) - 가짜 서버로 KOSPI/KOSDAQ 이중 등록·계좌 등록·ACK·데이터 전달·해제·재접속 흐름과 픽스처 파서 검증
(Kotlin LsMarketStreamTest 와 동일 시나리오)."""
import json
import queue

import pytest

pytest.importorskip("websockets")

from hermetix.brokers.ls_stream import LsMarketStream, parse_ls_order_book, parse_ls_order_events, parse_ls_trade  # noqa: E402
from tests.ws_fake_server import FakeServer, TODAY, assert_books, assert_events, assert_ticks, stream_section, wait_until  # noqa: E402

SECTION = stream_section("ls")
TRADE_FRAME = SECTION["frames"][0]
BOOK_FRAME = SECTION["orderBook"]["frames"][0]
SC0_FRAME, SC1_FRAME, SC3_FRAME = SECTION["orderEvents"]["frames"]
ACK = '{"header":{"tr_cd":"S3_","tr_key":"005930","tr_type":"3","rsp_cd":"00000","rsp_msg":"정상처리되었습니다"}}'


@pytest.fixture
def server():
    s = FakeServer()
    yield s
    s.shutdown()


def ls_stream(server) -> LsMarketStream:
    return LsMarketStream(server.url("/websocket"), lambda: "ACCESS-TOKEN")


def test_one_symbol_registers_kospi_and_kosdaq_trs_and_delivers_both(server):
    ticks: queue.Queue = queue.Queue()
    stream = ls_stream(server)
    try:
        stream.subscribe_trades(["KRX:005930"], ticks.put)
        stream.connect()
        conn = server.take()
        regs = [conn.take_json() for _ in range(2)]
        assert [r["body"]["tr_cd"] for r in regs] == ["S3_", "K3_"]
        assert all(r["header"] == {"token": "ACCESS-TOKEN", "tr_type": "3"} and r["body"]["tr_key"] == "005930" for r in regs)

        conn.send(ACK)
        conn.send(TRADE_FRAME)
        tick = ticks.get(timeout=5)
        assert tick.symbol == "KRX:005930"
        assert str(tick.price) == "55550" and str(tick.quantity) == "1" and tick.cumulative_volume == 10887
        assert str(tick.change) == "1050" and str(tick.change_rate) == "0.019300"
        assert str(tick.ask_price) == "55600" and str(tick.bid_price) == "55500"

        kosdaq = json.loads(TRADE_FRAME)
        kosdaq["header"]["tr_cd"] = "K3_"
        conn.send(json.dumps(kosdaq))
        assert ticks.get(timeout=5).symbol == "KRX:005930"
    finally:
        stream.close()


def test_order_events_register_sc0_to_sc4_and_deliver(server):
    events: queue.Queue = queue.Queue()
    stream = ls_stream(server)
    try:
        stream.subscribe_order_events(events.put)
        stream.connect()
        conn = server.take()
        regs = [conn.take_json() for _ in range(5)]
        assert [r["body"]["tr_cd"] for r in regs] == ["SC0", "SC1", "SC2", "SC3", "SC4"]
        assert all(r["header"]["tr_type"] == "1" and r["body"]["tr_key"] == "" for r in regs)

        for frame in (SC0_FRAME, SC1_FRAME, SC3_FRAME):
            conn.send(frame)
        accepted = events.get(timeout=5)
        assert accepted.type.name == "ACCEPTED" and accepted.order_id == "86382" and accepted.side.name == "BUY"
        assert accepted.quantity == 2 and accepted.price == 60000 and accepted.symbol == "005930"
        filled = events.get(timeout=5)
        assert filled.type.name == "FILLED" and filled.quantity == 1 and filled.price == 60000 and filled.remaining_quantity == 1
        canceled = events.get(timeout=5)
        assert canceled.type.name == "CANCELED" and canceled.order_id == "88343" and canceled.original_order_id == "88342"
        assert canceled.symbol == "000020"
    finally:
        stream.close()


def test_order_book_registers_h1_and_ha_and_delivers_ten_levels(server):
    books: queue.Queue = queue.Queue()
    stream = ls_stream(server)
    try:
        stream.subscribe_order_book(["005930"], books.put)
        stream.connect()
        conn = server.take()
        assert [conn.take_json()["body"]["tr_cd"] for _ in range(2)] == ["H1_", "HA_"]
        conn.send(BOOK_FRAME)
        book = books.get(timeout=5)
        assert len(book.asks) == 10 and len(book.bids) == 10
        assert (str(book.best_ask.price), str(book.best_ask.quantity)) == ("72400", "32616")
        assert (str(book.best_bid.price), str(book.best_bid.quantity)) == ("72300", "70581")
        assert str(book.total_ask_quantity) == "400000" and str(book.total_bid_quantity) == "500000"
    finally:
        stream.close()


def test_reconnect_resends_all_seven_registrations(server):
    stream = ls_stream(server)
    try:
        stream.subscribe_trades(["005930"], lambda t: None)
        stream.subscribe_order_events(lambda e: None)
        stream.connect()
        first = server.take()
        first.drain(7)
        first.close()
        second = server.take(10)
        codes = sorted(m["body"]["tr_cd"] for m in (second.take_json() for _ in range(7)))
        assert codes == ["K3_", "S3_", "SC0", "SC1", "SC2", "SC3", "SC4"]
    finally:
        stream.close()


def test_late_subscribe_and_unsubscribe(server):
    stream = ls_stream(server)
    try:
        stream.connect()
        conn = server.take()
        wait_until(lambda: stream.is_connected)
        stream.subscribe_trades(["000660"], lambda t: None)
        assert [conn.take_json()["body"] for _ in range(2)] == [{"tr_cd": "S3_", "tr_key": "000660"}, {"tr_cd": "K3_", "tr_key": "000660"}]
        stream.unsubscribe_trades(["000660"])
        assert [conn.take_json()["header"]["tr_type"] for _ in range(2)] == ["4", "4"]
        stream.subscribe_order_events(lambda e: None)
        conn.drain(5)
        stream.unsubscribe_order_events()
        assert [conn.take_json()["header"]["tr_type"] for _ in range(5)] == ["2"] * 5
    finally:
        stream.close()


def test_parsers_ignore_control_frames_and_map_kinds():
    ack = json.loads(ACK)
    assert parse_ls_trade(ack, TODAY) is None and parse_ls_order_book(ack, TODAY) is None and parse_ls_order_events(ack, TODAY) == []
    sc1 = json.loads(SC1_FRAME)
    modified = parse_ls_order_events({"header": {"tr_cd": "SC2"}, "body": {**sc1["body"], "ordxctptncode": "12", "mdfycnfqty": "1", "mdfycnfprc": "70000", "orgordno": "86382", "ordno": "86383"}}, TODAY)[0]
    assert modified.type.name == "MODIFIED" and modified.quantity == 1 and modified.price == 70000 and modified.original_order_id == "86382"
    rejected = parse_ls_order_events({"header": {"tr_cd": "SC4"}, "body": {**sc1["body"], "ordxctptncode": "", "rjtqty": "2", "msgcode": "0123"}}, TODAY)[0]
    assert rejected.type.name == "REJECTED" and rejected.quantity == 2 and rejected.reason == "0123"
    precedence = parse_ls_order_events({"header": {"tr_cd": "SC1"}, "body": {**sc1["body"], "ordxctptncode": "13", "canccnfqty": "1"}}, TODAY)[0]
    assert precedence.type.name == "CANCELED" and precedence.quantity == 1
    falling = json.loads(TRADE_FRAME)
    falling["body"]["sign"] = "5"
    assert parse_ls_trade(falling, TODAY).change == -1050


def test_fixture_stream_sections_document_based():
    assert SECTION["measured"] is False and SECTION["channel"] == "TRADES"
    assert_ticks([parse_ls_trade(json.loads(f), TODAY) for f in SECTION["frames"]], SECTION["expected"])
    book = SECTION["orderBook"]
    assert book["measured"] is False
    assert_books([parse_ls_order_book(json.loads(f), TODAY) for f in book["frames"]], book["expected"])
    events = SECTION["orderEvents"]
    assert events["measured"] is False
    assert_events([e for f in events["frames"] for e in parse_ls_order_events(json.loads(f), TODAY)], events["expected"])
