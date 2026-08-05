"""어댑터 정규화 검증 - 실측으로 녹화한 응답(골든 픽스처)을 재생한다.

Kotlin 레퍼런스 구현과 같은 응답에 같은 정규화 결과가 나와야 한다.
"""
from decimal import Decimal

from hermetix import CandleInterval, KisClient, KiwoomClient, NextClient, OrderStatus


def _stub(client, responses):
    """(_call 대체) 미리 정의한 응답을 순서대로 반환."""
    queue = list(responses)

    def fake(*args, **kwargs):
        return queue.pop(0)
    return fake


# ------------------------------------------------------------------------ next

def test_next_quote_parsing(monkeypatch):
    client = NextClient("k", "s")
    monkeypatch.setattr(client, "_request", _stub(client, [
        # 2026-08-03 실측 응답
        {"quotes": [{"symbol": "AAPL", "price": "308.91", "bidPrice": None, "askPrice": None,
                     "volume": 132756799, "change": "-24.52", "changeRate": "-0.073539",
                     "timestamp": "2026-07-31T04:00:00Z"}]},
    ]))
    q = client.get_quotes(["AAPL"])[0]
    assert q.price == Decimal("308.91")
    assert q.bid_price is None
    assert q.change_rate == Decimal("-0.073539")
    assert q.volume == 132756799


def test_next_order_parsing(monkeypatch):
    client = NextClient("k", "s")
    monkeypatch.setattr(client, "_request", _stub(client, [
        {"orderId": "ord_8615a1f026", "clientOrderId": "smoke-test-001", "status": "SUBMITTED",
         "symbol": "AAPL", "side": "BUY", "orderType": "LIMIT", "quantity": "1",
         "limitPrice": "200", "filledQuantity": "0", "submittedAt": "2026-08-03T02:57:33Z"},
    ]))
    order = client.get_order("ord_8615a1f026")
    assert order.status == OrderStatus.SUBMITTED
    assert order.status.is_open
    assert order.limit_price == Decimal("200")


# ------------------------------------------------------------------------- kis

def test_kis_quote_percent_to_rate(monkeypatch):
    client = KisClient("k", "s", "50199202")
    monkeypatch.setattr(client, "_call", _stub(client, [
        # 2026-08-03 실측: prdy_ctrt 는 % 단위
        {"output": {"stck_prpr": "239500", "acml_vol": "27393580",
                    "prdy_vrss": "-23000", "prdy_ctrt": "-8.76"}},
    ]))
    q = client.get_quotes(["005930"])[0]
    assert q.price == Decimal("239500")
    assert q.change_rate == Decimal("-0.0876")


def test_kis_daily_candles_sorted_oldest_first(monkeypatch):
    client = KisClient("k", "s", "50199202")
    monkeypatch.setattr(client, "_call", _stub(client, [
        # KIS 는 최신순으로 반환한다
        {"output2": [
            {"stck_bsop_date": "20260731", "stck_oprc": "304.81", "stck_hgpr": "310.69",
             "stck_lwpr": "300", "stck_clpr": "308.91", "acml_vol": "132756799"},
            {"stck_bsop_date": "20260730", "stck_oprc": "333.1", "stck_hgpr": "334.75",
             "stck_lwpr": "329.59", "stck_clpr": "333.43", "acml_vol": "75028386"},
        ]},
    ]))
    candles = client.get_candles("005930", CandleInterval.DAY_1, 2)
    assert candles[0].timestamp < candles[1].timestamp  # 과거 -> 최신
    assert candles[-1].close == Decimal("308.91")


def test_kis_minute_candles_rejected():
    client = KisClient("k", "s", "50199202")
    try:
        client.get_candles("005930", CandleInterval.HOUR_1)
        raise AssertionError("should raise")
    except ValueError as e:
        assert "일봉" in str(e)


def test_kis_holdings_filters_zero_qty(monkeypatch):
    client = KisClient("k", "s", "50199202")
    monkeypatch.setattr(client, "_balance", _stub(client, [
        {"output1": [
            {"pdno": "005930", "hldg_qty": "2", "pchs_avg_pric": "281.7500", "prpr": "308",
             "evlu_amt": "616", "evlu_pfls_amt": "54", "evlu_pfls_rt": "9.64"},
            {"pdno": "000660", "hldg_qty": "0", "pchs_avg_pric": "0"},
        ]},
    ]))
    holdings = client.get_holdings()
    assert len(holdings) == 1
    assert holdings[0].quantity == Decimal("2")
    assert holdings[0].unrealized_pnl_rate == Decimal("0.0964")


def test_kis_tracked_order_fill_by_holdings_change(monkeypatch):
    """모의 서버가 주문 조회를 제공하지 않으므로 보유 수량 변화로 체결을 판정한다."""
    from hermetix.models import CreateOrderRequest, OrderSide, OrderType

    client = KisClient("k", "s", "50199202")
    holdings_qty = {"value": Decimal(2)}
    monkeypatch.setattr(client, "get_holdings", lambda: [
        type("H", (), {"symbol": "005930", "quantity": holdings_qty["value"]})()])
    monkeypatch.setattr(client, "_call", lambda *a, **k: {"output": {"ODNO": "0000035787"}})

    order = client.create_order(CreateOrderRequest(
        symbol="005930", side=OrderSide.BUY, order_type=OrderType.LIMIT,
        quantity=Decimal(1), limit_price=Decimal("185400")))
    assert client.get_order(order.order_id).status == OrderStatus.SUBMITTED

    holdings_qty["value"] = Decimal(3)  # 체결로 보유 증가
    assert client.get_order(order.order_id).status == OrderStatus.FILLED


# ---------------------------------------------------------------------- kiwoom

def test_kiwoom_sign_prefix_normalization(monkeypatch):
    client = KiwoomClient("k", "s")
    monkeypatch.setattr(client, "_call", _stub(client, [
        # 2026-08-03 실측: cur_prc 에 하락 부호 접두
        {"cur_prc": "-239500", "trde_qty": "27393580", "pred_pre": "-23000", "flu_rt": "-8.76"},
    ]))
    q = client.get_quotes(["005930"])[0]
    assert q.price == Decimal("239500")      # 부호 제거
    assert q.change == Decimal("-23000")     # 대비는 부호 유지
    assert q.change_rate == Decimal("-0.0876")


def test_kiwoom_zero_padded_amounts(monkeypatch):
    client = KiwoomClient("k", "s")
    monkeypatch.setattr(client, "_call", _stub(client, [
        # kt00001 실측: zero-padded
        {"entr": "000000100000000", "ord_alow_amt": "000000100000000"},
        # kt00018
        {"prsm_dpst_aset_amt": "000000100000000", "tot_evlt_amt": "000000000000000"},
    ]))
    account = client.get_account()
    assert account.cash == Decimal("100000000")
    assert account.portfolio_value == Decimal("100000000")


def test_kiwoom_candles(monkeypatch):
    client = KiwoomClient("k", "s")
    monkeypatch.setattr(client, "_call", _stub(client, [
        {"stk_dt_pole_chart_qry": [
            {"dt": "20260803", "open_pric": "248000", "high_pric": "249500",
             "low_pric": "238000", "cur_prc": "239500", "trde_qty": "27393580"},
            {"dt": "20260801", "open_pric": "245000", "high_pric": "250000",
             "low_pric": "244000", "cur_prc": "248000", "trde_qty": "20000000"},
        ]},
    ]))
    candles = client.get_candles("005930", CandleInterval.DAY_1)
    assert candles[0].timestamp < candles[1].timestamp
    assert candles[-1].close == Decimal("239500")


def test_kiwoom_holdings_strips_a_prefix(monkeypatch):
    client = KiwoomClient("k", "s")
    monkeypatch.setattr(client, "_balance", _stub(client, [
        {"acnt_evlt_remn_indv_tot": [
            {"stk_cd": "A005930", "rmnd_qty": "000000000001", "pur_pric": "000000239500",
             "cur_prc": "-240000", "evlt_amt": "000000240000", "evltv_prft": "500", "prft_rt": "0.21"},
        ]},
    ]))
    h = client.get_holdings()[0]
    assert h.symbol == "005930"
    assert h.current_price == Decimal("240000")
