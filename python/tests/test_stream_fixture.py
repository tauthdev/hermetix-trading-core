"""골든 픽스처의 stream 섹션 - 네 언어가 같은 프레임을 같은 TradeTick 으로 파싱하는지 (Kotlin StreamFixtureTest 와 동일)."""
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from hermetix.brokers.kis_stream import parse_kis_frame, parse_kis_order_book, parse_kis_order_events
from hermetix.brokers.kiwoom_stream import parse_kiwoom_order_book, parse_kiwoom_order_events, parse_kiwoom_real

FIXTURES = Path(__file__).resolve().parents[2] / "conformance" / "fixtures"
KST = ZoneInfo("Asia/Seoul")
TODAY = date(2026, 9, 14)


def stream_section(broker: str) -> dict:
    fx = json.loads((FIXTURES / f"{broker}.json").read_text())
    assert "stream" in fx, f"{broker} 픽스처에 stream 섹션이 없다"
    return fx["stream"]


def assert_matches(ticks, expected):
    assert len(ticks) == len(expected)
    for tick, e in zip(ticks, expected):
        assert tick.symbol == e["symbol"]
        assert tick.price == Decimal(e["price"])
        assert tick.quantity == Decimal(e["quantity"])
        assert tick.ask_price == Decimal(e["askPrice"])
        assert tick.bid_price == Decimal(e["bidPrice"])
        assert tick.cumulative_volume == e["cumulativeVolume"]
        assert tick.change == Decimal(e["change"])
        assert tick.change_rate == Decimal(e["changeRate"])
        assert tick.timestamp.astimezone(KST).strftime("%H:%M:%S") == e["time"]


def test_kis_h0stcnt0_frames():
    section = stream_section("kis")
    assert section["channel"] == "TRADES"
    ticks = [t for frame in section["frames"] for t in parse_kis_frame(frame, TODAY)]
    assert_matches(ticks, section["expected"])


def test_kiwoom_real_0b_frames():
    section = stream_section("kiwoom")
    assert section["channel"] == "TRADES"
    ticks = [t for frame in section["frames"] for t in parse_kiwoom_real(json.loads(frame), TODAY)]
    assert_matches(ticks, section["expected"])


@pytest.mark.parametrize("frame", [
    "0|H0STASP0|001|005930^093012^71500",   # 다른 TR
    "0|H0STCNT0|001|005930^093012",         # 필드 부족
    "garbage",
])
def test_kis_parser_ignores_unknown_frames(frame):
    assert parse_kis_frame(frame, TODAY) == []


def test_kis_multi_record_and_sign_code():
    fields = "005930^093012^71500^5^300^-0.42^71480^71800^71900^71300^71500^71400^15^1234567^0^0^0^0^0^0"
    one = parse_kis_frame(f"0|H0STCNT0|001|{fields}", TODAY)[0]
    assert one.change == Decimal("-300")  # 부호코드 5(하락) + 무부호 값 -> 음수
    assert one.change_rate == Decimal("-0.0042")
    assert len(parse_kis_frame(f"0|H0STCNT0|002|{fields}^{fields}", TODAY)) == 2


def test_kiwoom_parser_skips_other_types_and_strips_prefix():
    real = {"trnm": "REAL", "data": [
        {"type": "0D", "name": "주식호가잔량", "item": "005930", "values": {"21": "093012"}},
        {"type": "0B", "name": "주식체결", "item": "A005930",
         "values": {"20": "093012", "10": "-71500", "11": "-300", "12": "-0.42", "27": "+71500", "28": "+71400", "15": "-15", "13": "1234567"}},
    ]}
    ticks = parse_kiwoom_real(real, TODAY)
    assert [t.symbol for t in ticks] == ["005930"]
    assert ticks[0].price == Decimal(71500) and ticks[0].quantity == Decimal(15)
    assert ticks[0].ask_price == Decimal(71500) and ticks[0].bid_price == Decimal(71400)


# ------------------------------------------------------------------ 2차 채널: 호가 (실측) · 주문 통보 (문서 기반)

def assert_books(books, expected):
    assert len(books) == len(expected)
    for book, e in zip(books, expected):
        assert book.symbol == e["symbol"]
        assert book.timestamp.astimezone(KST).strftime("%H:%M:%S") == e["time"]
        assert [(str(l.price), str(l.quantity)) for l in book.asks] == [(l["price"], l["quantity"]) for l in e["asks"]]
        assert [(str(l.price), str(l.quantity)) for l in book.bids] == [(l["price"], l["quantity"]) for l in e["bids"]]
        assert book.total_ask_quantity == Decimal(e["totalAskQuantity"])
        assert book.total_bid_quantity == Decimal(e["totalBidQuantity"])


def assert_events(events, expected):
    assert len(events) == len(expected)
    for ev, e in zip(events, expected):
        assert ev.order_id == e["orderId"]
        assert ev.type.name == e["type"]
        assert ev.timestamp.astimezone(KST).strftime("%H:%M:%S") == e["time"]
        assert ev.symbol == e["symbol"]
        assert ev.side.name == e["side"]
        assert ev.quantity == Decimal(e["quantity"])
        assert ev.price == Decimal(e["price"])
        if "remainingQuantity" in e:
            assert ev.remaining_quantity == Decimal(e["remainingQuantity"])
        assert ev.original_order_id == e.get("originalOrderId")


def test_kis_h0stasp0_order_book_frames_measured():
    section = stream_section("kis")["orderBook"]
    assert section["channel"] == "ORDER_BOOK"
    assert_books([b for f in section["frames"] for b in parse_kis_order_book(f, TODAY)], section["expected"])


def test_kis_h0stcni9_order_event_frames_document_based():
    section = stream_section("kis")["orderEvents"]
    assert section["measured"] is False
    assert_events([e for f in section["frames"] for e in parse_kis_order_events(f, TODAY)], section["expected"])


def test_kiwoom_0d_order_book_frames_measured():
    section = stream_section("kiwoom")["orderBook"]
    assert_books([b for f in section["frames"] for b in parse_kiwoom_order_book(json.loads(f), TODAY)], section["expected"])


def test_kiwoom_00_order_event_frames_document_based():
    section = stream_section("kiwoom")["orderEvents"]
    assert section["measured"] is False
    assert_events([e for f in section["frames"] for e in parse_kiwoom_order_events(json.loads(f), TODAY)], section["expected"])


def test_kis_order_event_parser_cancel_reject_and_wrong_tr():
    canceled = "U^A^0000000002^0000000001^01^2^00^0^005930^0^0^105530^0^1^2^00950^3^N^0^^N^^^^S^0"
    rejected = "U^A^0000000003^0000000000^02^0^00^0^005930^0^0^105530^1^1^1^00950^3^N^0^^N^^^^S^0"
    c = parse_kis_order_events(f"0|H0STCNI9|001|{canceled}", TODAY)[0]
    assert c.type.name == "CANCELED" and c.side.name == "SELL" and c.original_order_id == "0000000001"
    r = parse_kis_order_events(f"0|H0STCNI0|001|{rejected}", TODAY)[0]
    assert r.type.name == "REJECTED"
    assert parse_kis_order_events(f"0|H0STCNT0|001|{canceled}", TODAY) == []
    assert c.order_id_matches("2") and not c.order_id_matches("3")


def test_kiwoom_order_event_parser_accept_cancel_modify_reject():
    def frame(status, kind, filled="0", reason=""):
        return {"trnm": "REAL", "data": [{"type": "00", "name": "주문체결", "item": "A005930", "values": {
            "9203": "0000012346", "904": "0000000000", "9001": "A005930", "913": status, "905": kind, "907": "2",
            "900": "1", "901": "+240000", "902": "1", "910": "", "911": filled, "908": "105530", "919": reason}}]}
    accepted = parse_kiwoom_order_events(frame("접수", "+매수"), TODAY)[0]
    assert accepted.type.name == "ACCEPTED" and accepted.quantity == 1 and accepted.price == 240000
    assert accepted.original_order_id is None
    assert parse_kiwoom_order_events(frame("확인", "매수취소"), TODAY)[0].type.name == "CANCELED"
    assert parse_kiwoom_order_events(frame("확인", "매수정정"), TODAY)[0].type.name == "MODIFIED"
    rejected = parse_kiwoom_order_events(frame("거부", "+매수", reason="주문가능금액 부족"), TODAY)[0]
    assert rejected.type.name == "REJECTED" and rejected.reason == "주문가능금액 부족"
