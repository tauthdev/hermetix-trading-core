"""토스증권 웹소켓 스트림 (AsyncAPI 1.2.2 기반) - 가짜 서버로 Bearer 핸드셰이크·선언형 구독(합치기·거부 제외)·데이터 전달·PING·재접속 흐름과
픽스처 파서 검증 (Kotlin TossMarketStreamTest 와 동일 시나리오)."""
import json
import queue
from decimal import Decimal

import pytest

pytest.importorskip("websockets")

from hermetix.brokers.toss_stream import (TossMarketStream, parse_toss_order_book, parse_toss_order_event,  # noqa: E402
                                          parse_toss_trade, topic_key)
from tests.ws_fake_server import FakeServer, assert_books, assert_events, assert_ticks, stream_section, wait_until  # noqa: E402

SECTION = stream_section("toss")
TRADE_FRAME = SECTION["frames"][0]
BOOK_FRAME = SECTION["orderBook"]["frames"][0]
ORDER_FRAME = SECTION["orderEvents"]["frames"][0]


@pytest.fixture
def server():
    s = FakeServer()
    yield s
    s.shutdown()


def toss_stream(server, heartbeat_seconds: float = 0.0) -> TossMarketStream:
    return TossMarketStream(server.url("/ws/v1"), lambda: "ACCESS-TOKEN", lambda: "3",
                            heartbeat_seconds=heartbeat_seconds, declare_delay_seconds=0.05)


def _by_type(declaration: list) -> dict:
    return {item["type"]: item["codes"] for item in declaration if "type" in item}


def test_bearer_handshake_and_single_coalesced_declaration(server):
    ticks: queue.Queue = queue.Queue()
    books: queue.Queue = queue.Queue()
    stream = toss_stream(server)
    try:
        stream.subscribe_trades(["KRX:005930"], ticks.put)
        stream.subscribe_trades(["US:AAPL"], ticks.put)
        stream.subscribe_order_book(["005930"], books.put)
        stream.subscribe_order_events(lambda e: None)
        stream.connect()
        conn = server.take()
        assert conn.headers.get("authorization") == "Bearer ACCESS-TOKEN"
        declaration = conn.take_json()
        assert declaration[0]["id"].startswith("req-")
        assert _by_type(declaration) == {"trade:kr": ["005930"], "trade:us": ["AAPL"], "orderbook:kr": ["005930"], "personal:order": ["3"]}

        conn.send('{"type":"subscriptions","id":"req-1","subscribed":["trade:kr:005930","trade:us:AAPL","orderbook:kr:005930","personal:order:3"],"rejected":[]}')
        conn.send('{"type":"message","topic":"trade:kr:005930","data":{"price":"71500","volume":"15","timestamp":"2026-09-14T09:30:12.000+09:00","currency":"KRW"}}')
        tick = ticks.get(timeout=5)
        assert tick.symbol == "KRX:005930" and tick.price == 71500 and tick.quantity == 15 and tick.cumulative_volume is None
        conn.send(TRADE_FRAME)
        us = ticks.get(timeout=5)
        assert us.symbol == "US:AAPL" and str(us.price) == "243.26"
        conn.send(BOOK_FRAME)
        book = books.get(timeout=5)
        # 같은 topic 은 먼저 구독한 표기(KRX:005930)를 돌려준다 - Kotlin 과 같은 요청 표기 맵 공유 규칙
        assert book.symbol == "KRX:005930" and str(book.best_ask.price) == "71500" and str(book.best_bid.quantity) == "10"
    finally:
        stream.close()


def test_late_subscribe_redeclares_and_rejected_target_is_dropped(server):
    stream = toss_stream(server)
    try:
        stream.subscribe_trades(["005930"], lambda t: None)
        stream.connect()
        conn = server.take()
        conn.take_json()
        wait_until(lambda: stream.is_connected)
        stream.subscribe_trades(["999999"], lambda t: None)
        assert _by_type(conn.take_json())["trade:kr"] == ["005930", "999999"]
        conn.send('{"type":"subscriptions","subscribed":["trade:kr:005930"],"rejected":[{"target":"trade:kr:999999","code":"stock-not-found","message":"없음"}]}')
        wait_until(lambda: "trade:kr:999999" in stream._rejected)
        stream.subscribe_order_book(["005930"], lambda b: None)
        declaration = _by_type(conn.take_json())
        assert declaration["trade:kr"] == ["005930"] and declaration["orderbook:kr"] == ["005930"]
    finally:
        stream.close()


def test_order_events_fill_and_partial_fill_delta(server):
    events: queue.Queue = queue.Queue()
    stream = toss_stream(server)
    try:
        stream.subscribe_order_events(events.put)
        stream.connect()
        conn = server.take()
        conn.take_json()
        conn.send(ORDER_FRAME)
        filled = events.get(timeout=5)
        assert filled.type.name == "FILLED" and filled.quantity == 10 and filled.price == 100 and filled.remaining_quantity == 0
        assert filled.symbol == "US:AAPL" and filled.side.name == "BUY"

        base = json.loads(ORDER_FRAME)
        base["data"]["order"]["orderId"] = "ord-2"
        base["data"]["event"] = "PARTIAL_FILL"
        base["data"]["order"]["execution"]["filledQuantity"] = "4"
        conn.send(json.dumps(base))
        base["data"]["event"] = "FILL"
        base["data"]["order"]["execution"]["filledQuantity"] = "10"
        conn.send(json.dumps(base))
        assert events.get(timeout=5).quantity == 4
        second = events.get(timeout=5)
        assert second.quantity == 6 and second.remaining_quantity == 0

        base["data"]["event"] = "CANCELING"
        conn.send(json.dumps(base))
        base["data"]["event"] = "CANCEL_REJECTED"
        conn.send(json.dumps(base))
        rejected = events.get(timeout=5)
        assert rejected.type.name == "REJECTED" and rejected.reason == "CANCEL_REJECTED"
    finally:
        stream.close()


def test_heartbeat_sends_raw_ping_and_pong_error_are_swallowed(server):
    stream = toss_stream(server, heartbeat_seconds=0.1)
    try:
        stream.subscribe_trades(["005930"], lambda t: None)
        stream.connect()
        conn = server.take()
        conn.take_json()
        assert conn.take() == "PING"
        conn.send('{"type":"pong"}')
        conn.send('{"type":"error","error":{"code":"rate-limit-exceeded","message":"slow down"}}')
        assert conn.take() == "PING"
        assert stream.is_connected
    finally:
        stream.close()


def test_reconnect_redeclares(server):
    stream = toss_stream(server)
    try:
        stream.subscribe_trades(["005930"], lambda t: None)
        stream.connect()
        first = server.take()
        first.take_json()
        first.close()
        second = server.take(10)
        assert _by_type(second.take_json()) == {"trade:kr": ["005930"]}
    finally:
        stream.close()


def test_topic_key_rules():
    assert topic_key("005930") == "kr:005930"
    assert topic_key("KRX:005930") == "kr:005930"
    assert topic_key("US:aapl") == "us:AAPL"
    with pytest.raises(ValueError):
        topic_key("JP:7203")


def test_parsers():
    assert parse_toss_trade("orderbook:kr:005930", {"price": "1"}) is None
    assert parse_toss_trade("trade:kr:005930", {"volume": "1"}) is None
    assert parse_toss_order_book("orderbook:us:AAPL", {"asks": [{"price": "1.5", "volume": "2"}], "bids": []}).symbol == "US:AAPL"
    data = json.loads(ORDER_FRAME)["data"]
    assert parse_toss_order_event({**data, "event": "REPLACING"}, None) is None
    assert parse_toss_order_event({**data, "event": "PENDING"}, None).type.name == "ACCEPTED"
    assert parse_toss_order_event({**data, "event": "REPLACED"}, None).type.name == "MODIFIED"
    canceled = parse_toss_order_event({**data, "event": "CANCELED"}, Decimal(3))
    assert canceled.type.name == "CANCELED" and canceled.quantity == 10


def test_fixture_stream_sections_document_based():
    assert SECTION["measured"] is False and SECTION["channel"] == "TRADES"
    ticks = [parse_toss_trade(json.loads(f)["topic"], json.loads(f)["data"]) for f in SECTION["frames"]]
    assert_ticks(ticks, SECTION["expected"])
    book = SECTION["orderBook"]
    assert book["measured"] is False
    assert_books([parse_toss_order_book(json.loads(f)["topic"], json.loads(f)["data"]) for f in book["frames"]], book["expected"])
    events = SECTION["orderEvents"]
    assert events["measured"] is False
    assert_events([parse_toss_order_event(json.loads(f)["data"], None) for f in events["frames"]], events["expected"])
