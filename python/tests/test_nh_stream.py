"""NH PLUG 웹소켓 스트림 (문서 기반) - 가짜 서버로 구독·ACK·프레임 전달·재접속 흐름과 픽스처 파서 검증 (Kotlin NhMarketStreamTest 와 동일 시나리오)."""
import json
import queue

import pytest

pytest.importorskip("websockets")

from hermetix.brokers.nh_stream import NhMarketStream, parse_nh_order_book, parse_nh_order_events, parse_nh_trade  # noqa: E402
from tests.ws_fake_server import (FakeServer, TODAY, assert_books, assert_events, assert_ticks, stream_section,  # noqa: E402
                                  wait_until)

SECTION = stream_section("nh")
TRADE_FRAME = SECTION["frames"][0]
BOOK_FRAME = SECTION["orderBook"]["frames"][0]
D3_FRAME, D2_FRAME = SECTION["orderEvents"]["frames"]


@pytest.fixture
def server():
    s = FakeServer()
    yield s
    s.shutdown()


def nh_stream(server, market_cd: str = "UNT", account_no: str = "") -> NhMarketStream:
    return NhMarketStream(server.url("/websocket"), lambda: "ACCESS-TOKEN", market_cd=market_cd, account_no=account_no)


def test_subscribe_carries_token_and_unt_channel_and_delivers_trade_in_requested_notation(server):
    ticks: queue.Queue = queue.Queue()
    stream = nh_stream(server)
    try:
        stream.subscribe_trades(["KRX:005940"], ticks.put)
        stream.connect()
        conn = server.take()
        subscribe = conn.take_json()
        assert subscribe == {"header": {"token": "ACCESS-TOKEN", "tr_type": "1"}, "body": {"tr_cd": "mc", "tr_key": "005940"}}

        conn.send('{"header":{"tr_type":"1","tr_cd":"mc","rsp_cd":"00000","rsp_msg":"정상처리되었습니다"},"body":{"tr_key":["005940"]}}')
        conn.send(TRADE_FRAME)
        tick = ticks.get(timeout=5)
        assert tick.symbol == "KRX:005940"
        assert str(tick.price) == "31750" and str(tick.quantity) == "13" and tick.cumulative_volume == 837624
        assert str(tick.change) == "2500" and str(tick.change_rate) == "0.085500"
        assert str(tick.ask_price) == "31750" and str(tick.bid_price) == "31700"
        assert stream.is_connected
    finally:
        stream.close()


def test_krx_config_uses_oc_and_ob_channels(server):
    stream = nh_stream(server, market_cd="KRX")
    try:
        stream.subscribe_trades(["005930"], lambda t: None)
        stream.subscribe_order_book(["005930"], lambda b: None)
        stream.connect()
        conn = server.take()
        channels = {m["body"]["tr_cd"] for m in (conn.take_json(), conn.take_json())}
        assert channels == {"oc", "ob"}
    finally:
        stream.close()


def test_order_book_frame_delivers_ten_levels(server):
    books: queue.Queue = queue.Queue()
    stream = nh_stream(server)
    try:
        stream.subscribe_order_book(["005940"], books.put)
        stream.connect()
        conn = server.take()
        assert conn.take_json()["body"] == {"tr_cd": "mb", "tr_key": "005940"}
        conn.send(BOOK_FRAME)
        book = books.get(timeout=5)
        assert book.symbol == "005940"
        assert len(book.asks) == 10 and len(book.bids) == 10
        assert (str(book.best_ask.price), str(book.best_ask.quantity)) == ("31800", "668")
        assert (str(book.best_bid.price), str(book.best_bid.quantity)) == ("31750", "2899")
        assert str(book.asks[9].price) == "32250"
        assert str(book.total_ask_quantity) == "33938" and str(book.total_bid_quantity) == "13132"
    finally:
        stream.close()


def test_order_events_register_d2_and_d3_and_deliver_accepted_then_filled(server):
    events: queue.Queue = queue.Queue()
    stream = nh_stream(server)
    try:
        stream.subscribe_order_events(events.put)
        stream.connect()
        conn = server.take()
        regs = [conn.take_json() for _ in range(2)]
        assert [r["body"] for r in regs] == [{"tr_cd": "d2", "tr_key": ""}, {"tr_cd": "d3", "tr_key": ""}]
        assert all(r["header"] == {"token": "ACCESS-TOKEN", "tr_type": "1"} for r in regs)

        conn.send(D3_FRAME)
        conn.send(D2_FRAME)
        accepted = events.get(timeout=5)
        assert accepted.type.name == "ACCEPTED" and accepted.order_id_matches("30")
        assert accepted.symbol == "005940" and accepted.side.name == "BUY"
        assert accepted.quantity == 10 and accepted.price == 35550 and accepted.original_order_id is None
        filled = events.get(timeout=5)
        assert filled.type.name == "FILLED" and filled.quantity == 5 and filled.price == 35550
    finally:
        stream.close()


def test_reconnect_resubscribes_all_groups(server):
    stream = nh_stream(server)
    try:
        stream.subscribe_trades(["005930"], lambda t: None)
        stream.subscribe_order_book(["005930"], lambda b: None)
        stream.subscribe_order_events(lambda e: None)
        stream.connect()
        first = server.take()
        first.drain(4)
        first.close()
        second = server.take(10)
        bodies = [m["body"]["tr_cd"] for m in (second.take_json() for _ in range(4))]
        assert sorted(bodies) == ["d2", "d3", "mb", "mc"]
    finally:
        stream.close()


def test_late_subscription_is_sent_immediately(server):
    stream = nh_stream(server)
    try:
        stream.connect()
        conn = server.take()
        wait_until(lambda: stream.is_connected)
        stream.subscribe_trades(["000660"], lambda t: None)
        assert conn.take_json()["body"] == {"tr_cd": "mc", "tr_key": "000660"}
    finally:
        stream.close()


def test_parser_accepts_json_numbers_hhmmss_and_falling_sign():
    node = {"header": {"tr_cd": "oc", "tr_key": "005930"},
            "body": {"code": "005930", "time": "093012", "sign": "5", "change": 300, "price": 71500, "chrate": 0.42,
                     "offer": "71500", "bid": "71400", "movolume": 15, "new_volume": 1234567}}
    tick = parse_nh_trade(node, TODAY)[0]
    assert tick.symbol == "005930" and tick.price == 71500 and tick.quantity == 15 and tick.cumulative_volume == 1234567
    assert tick.change == -300 and str(tick.change_rate) == "-0.004200"
    assert tick.timestamp.astimezone(__import__("zoneinfo").ZoneInfo("Asia/Seoul")).strftime("%H:%M:%S") == "09:30:12"
    assert parse_nh_trade({"header": {"tr_cd": "oc"}, "body": None}, TODAY) == []


def test_order_event_parser_filters_and_kinds():
    base = json.loads(D2_FRAME)
    assert parse_nh_order_events({"header": {"tr_cd": "d2"}, "body": {**base["body"], "itemgb": "2"}}, TODAY) == []
    assert parse_nh_order_events(base, TODAY, account_no="OTHER") == []
    assert parse_nh_order_events(base, TODAY, account_no="ACCOUNT NUMBER")[0].type.name == "FILLED"
    assert parse_nh_order_events({"header": {"tr_cd": "d2"}, "body": {**base["body"], "ucgb": "2"}}, TODAY)[0].type.name == "CANCELED"
    assert parse_nh_order_events({"header": {"tr_cd": "d2"}, "body": {**base["body"], "ucgb": "3"}}, TODAY)[0].type.name == "CANCELED"
    assert parse_nh_order_events({"header": {"tr_cd": "d2"}, "body": {**base["body"], "ucgb": "1"}}, TODAY)[0].type.name == "MODIFIED"
    assert parse_nh_order_events({"header": {"tr_cd": "d2"}, "body": {**base["body"], "rejgb": "1"}}, TODAY)[0].type.name == "REJECTED"
    d3 = json.loads(D3_FRAME)
    modify = parse_nh_order_events({"header": {"tr_cd": "d3"}, "body": {**d3["body"], "orgordno": "0000000029"}}, TODAY)[0]
    assert modify.type.name == "ACCEPTED" and modify.original_order_id == "0000000029"
    assert parse_nh_order_events({"header": {"tr_cd": "oc"}, "body": d3["body"]}, TODAY) == []


def test_fixture_stream_sections_document_based():
    assert SECTION["measured"] is False and SECTION["channel"] == "TRADES"
    assert_ticks([t for f in SECTION["frames"] for t in parse_nh_trade(json.loads(f), TODAY)], SECTION["expected"])
    book = SECTION["orderBook"]
    assert book["measured"] is False
    assert_books([b for f in book["frames"] for b in parse_nh_order_book(json.loads(f), TODAY)], book["expected"])
    events = SECTION["orderEvents"]
    assert events["measured"] is False
    assert_events([e for f in events["frames"] for e in parse_nh_order_events(json.loads(f), TODAY)], events["expected"])
