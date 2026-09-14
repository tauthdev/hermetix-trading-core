"""골든 픽스처의 stream 섹션 - 네 언어가 같은 프레임을 같은 TradeTick 으로 파싱하는지 (Kotlin StreamFixtureTest 와 동일)."""
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from hermetix.brokers.kis_stream import parse_kis_frame
from hermetix.brokers.kiwoom_stream import parse_kiwoom_real

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
