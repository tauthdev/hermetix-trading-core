"""어댑터 정규화 검증 - 실측으로 녹화한 응답(골든 픽스처)을 재생한다.

Kotlin 레퍼런스 구현과 같은 응답에 같은 정규화 결과가 나와야 한다.
"""
from decimal import Decimal

from hermetix import CandleInterval, CreateOrderRequest, KisClient, KiwoomClient, NextClient, OrderStatus


def _stub(client, responses):
    """(_call 대체) 미리 정의한 응답을 순서대로 반환."""
    queue = list(responses)

    def fake(*args, **kwargs):
        return queue.pop(0)
    return fake


# ------------------------------------------------------------------------ next

def test_next_quote_parsing(monkeypatch):
    # v1.3: outcome=OK 만 공통 모델로, 등락률 % → 비율, 시각 KST → aware datetime
    client = NextClient("k", "s")
    monkeypatch.setattr(client, "_request", _stub(client, [
        {"quotes": [
            {"symbol": "AAPL", "outcome": "OK", "session": "REGULAR", "requestedAt": "2026-09-10T23:10:00+09:00",
             "price": "308.91", "previousClose": "333.43", "change": "-24.52", "changeRate": "-7.3539",
             "bidPrice": None, "bidSize": None, "askPrice": None, "askSize": None,
             "volume": "132756799", "lastTradeAt": "2026-09-10T23:09:58+09:00"},
            {"symbol": "NOPE", "outcome": "NOT_FOUND", "session": "CLOSED", "requestedAt": "2026-09-10T23:10:00+09:00"},
        ]},
    ]))
    quotes = client.get_quotes(["AAPL", "NOPE"])
    assert [q.symbol for q in quotes] == ["AAPL"]
    q = quotes[0]
    assert q.price == Decimal("308.91")
    assert q.bid_price is None
    assert q.change_rate == Decimal("-0.073539")
    assert q.volume == 132756799
    assert q.timestamp.utcoffset().total_seconds() == 9 * 3600
    assert q.timestamp.isoformat() == "2026-09-10T23:09:58+09:00"


def test_next_candle_time_without_offset_is_kst(monkeypatch):
    client = NextClient("k", "s")
    monkeypatch.setattr(client, "_request", _stub(client, [
        {"symbol": "AAPL", "interval": "1d", "candles": [
            {"time": "2026-09-09T22:30:00", "session": "REGULAR", "open": "339.73", "high": "344.56",
             "low": "337.35", "close": "338.19", "volume": "56298904"}], "nextCursor": None},
    ]))
    c = client.get_candles("AAPL", CandleInterval.DAY_1, 3)[0]
    assert c.timestamp.isoformat() == "2026-09-09T22:30:00+09:00"
    assert c.volume == 56298904


def test_next_calendar_kst_sessions_to_new_york_clock(monkeypatch):
    client = NextClient("k", "s")
    monkeypatch.setattr(client, "_request", _stub(client, [
        {"calendar": [
            {"date": "2026-09-10", "status": "OPEN", "holidayName": None, "sessions": [
                {"type": "PRE", "open": "2026-09-10T17:00:00+09:00", "close": "2026-09-10T22:30:00+09:00"},
                {"type": "REGULAR", "open": "2026-09-10T22:30:00+09:00", "close": "2026-09-11T05:00:00+09:00"}]},
            {"date": "2026-11-27", "status": "HALF_DAY", "holidayName": "Day After Thanksgiving", "sessions": [
                {"type": "REGULAR", "open": "2026-11-27T23:30:00+09:00", "close": "2026-11-28T03:00:00+09:00"}]},
            {"date": "2026-12-25", "status": "CLOSED", "holidayName": "Christmas", "sessions": []},
        ]},
    ]))
    days = client.get_calendar()
    assert days[0].open and days[0].timezone == "America/New_York"
    assert (days[0].regular.start, days[0].regular.end) == ("09:30", "16:00")
    assert days[1].open and days[1].regular.end == "13:00"  # 반장일 (EST)
    assert days[1].holiday == "Day After Thanksgiving"
    assert not days[2].open and days[2].regular is None


def test_next_account_portfolio_value_is_cash_plus_holdings(monkeypatch):
    client = NextClient("k", "s")
    monkeypatch.setattr(client, "_request", _stub(client, [
        {"accountId": "acc_main", "currency": "USD", "cashAmount": "1000.50", "requestedAt": "2026-09-10T23:10:00+09:00"},
        {"currency": "USD", "holdings": [
            {"symbol": "AAPL", "name": "Apple Inc.", "quantity": "2", "sellableQuantity": "2", "averageBuyPrice": "300.00",
             "currentPrice": "310.00", "purchaseAmount": "600.00", "evaluationAmount": "620.00",
             "evaluationPnl": "20.00", "evaluationPnlRate": "3.3333"}], "requestedAt": "2026-09-10T23:10:00+09:00"},
    ]))
    account = client.get_account()
    assert account.cash == Decimal("1000.50")
    assert account.portfolio_value == Decimal("1620.50")


def test_next_holdings_mapping(monkeypatch):
    client = NextClient("k", "s")
    monkeypatch.setattr(client, "_request", _stub(client, [
        {"currency": "USD", "holdings": [
            {"symbol": "AAPL", "name": "Apple Inc.", "quantity": "2", "sellableQuantity": "1", "averageBuyPrice": "300.00",
             "currentPrice": "310.00", "purchaseAmount": "600.00", "evaluationAmount": "620.00",
             "evaluationPnl": "20.00", "evaluationPnlRate": "3.3333"}], "requestedAt": "2026-09-10T23:10:00+09:00"},
    ]))
    h = client.get_holdings()[0]
    assert h.avg_entry_price == Decimal("300.00")
    assert h.market_value == Decimal("620.00")
    assert h.unrealized_pnl == Decimal("20.00")
    assert h.unrealized_pnl_rate == Decimal("0.033333")


def test_next_order_parsing(monkeypatch):
    # v1.3 상세: requestId = clientOrderId, requestedAt = 접수 시각
    client = NextClient("k", "s")
    monkeypatch.setattr(client, "_request", _stub(client, [
        {"orderId": "ord_8615a1f026", "requestId": "smoke-test-001", "market": "US", "status": "SUBMITTED",
         "symbol": "AAPL", "side": "BUY", "orderType": "LIMIT", "timeInForce": "DAY", "quantity": "1",
         "limitPrice": "200", "filledQuantity": "0", "requestedAt": "2026-09-10T23:10:00+09:00",
         "updatedAt": "2026-09-10T23:10:00+09:00"},
    ]))
    order = client.get_order("ord_8615a1f026")
    assert order.status == OrderStatus.SUBMITTED
    assert order.status.is_open
    assert order.limit_price == Decimal("200")
    assert order.client_order_id == "smoke-test-001"
    assert order.submitted_at.isoformat() == "2026-09-10T23:10:00+09:00"


def test_next_create_order_sends_v13_body(monkeypatch):
    from hermetix import OrderSide, OrderType
    client = NextClient("k", "s")
    sent: list[dict] = []

    def fake_request(method, path, *, query=None, json_body=None, account=False):
        sent.append(json_body)
        return {"orderId": "ord_1", "market": "US", "status": "SUBMITTED", "requestedAt": "2026-09-10T23:10:00+09:00"}

    monkeypatch.setattr(client, "_request", fake_request)
    order = client.create_order(CreateOrderRequest("AAPL", OrderSide.BUY, OrderType.LIMIT, Decimal(1),
                                                   limit_price=Decimal(200), client_order_id="s-1"))
    assert order.status == OrderStatus.SUBMITTED
    body = sent[0]
    assert body["market"] == "US" and body["clientOrderId"] == "s-1"
    assert body["orderType"] == "LIMIT" and body["quantity"] == "1" and body["limitPrice"] == "200"


def test_next_pending_cancel_is_open(monkeypatch):
    # v1.3 부록 D: 취소 접수 후 미확정 — 원주문 체결 가능성이 있어 OPEN
    client = NextClient("k", "s")
    monkeypatch.setattr(client, "_request", _stub(client, [
        {"orderId": "ord_1", "status": "PENDING_CANCEL", "symbol": "AAPL", "side": "BUY",
         "orderType": "LIMIT", "quantity": "1", "limitPrice": "200", "filledQuantity": "0"},
    ]))
    order = client.cancel_order("ord_1")
    assert order.status == OrderStatus.PENDING_CANCEL
    assert order.status.is_open


def test_next_v13_headers(monkeypatch):
    # v1.3 공통 헤더: X-Request-Id(전 API), X-Next-Account-Id(계좌 그룹)
    client = NextClient("k", "s", account_id="acc_main")
    seen: list[dict] = []

    def fake_http(method, path, *, headers=None, query=None, json_body=None, form_body=None):
        seen.append({"method": method, "path": path, "headers": dict(headers or {})})
        if path == "/v1/oauth/token":
            return 200, {"access_token": "tok", "token_type": "Bearer", "expires_in": 43200}
        if path == "/v1/account/holdings":
            return 200, {"currency": "USD", "holdings": []}
        return 200, {"accountId": "acc_main", "currency": "USD", "cashAmount": "1"}

    monkeypatch.setattr(client._http, "request", fake_http)
    client.get_account()

    token_call, account_call, _holdings_call = seen
    assert token_call["path"] == "/v1/oauth/token"
    assert "X-Nextsecurities-Account" not in account_call["headers"]
    assert account_call["headers"]["X-Next-Account-Id"] == "acc_main"
    rid = account_call["headers"]["X-Request-Id"]
    assert len(rid) <= 64 and all(ch.isalnum() or ch in "._-" for ch in rid)


def test_next_token_oauth_error_format(monkeypatch):
    # 토큰 발급 400/401 만 OAuth 표준 {error, error_description}
    from hermetix import AuthError
    client = NextClient("k", "s")
    monkeypatch.setattr(client._http, "request",
                        lambda *a, **k: (401, {"error": "invalid_client", "error_description": "인증 실패"}))
    try:
        client.get_quotes(["AAPL"])
    except AuthError as e:
        assert e.error_code == "invalid_client"
        assert "인증 실패" in str(e)
    else:
        raise AssertionError("AuthError expected")


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


# ------------------------------------------------------------- 0.6.0: 환경 · 심볼 접두

def test_next_environment_must_match_key_prefix():
    from hermetix import TradingEnvironment
    import pytest
    with pytest.raises(ValueError):
        NextClient("pk_test_x", "s", environment=TradingEnvironment.LIVE)
    with pytest.raises(ValueError):
        NextClient("pk_live_x", "s", environment=TradingEnvironment.PAPER)
    assert NextClient("pk_live_x", "s", environment=TradingEnvironment.LIVE).environment == TradingEnvironment.LIVE
    assert NextClient("k", "s").environment == TradingEnvironment.PAPER


def test_next_market_prefixed_symbol(monkeypatch):
    import pytest
    client = NextClient("k", "s")
    seen = {}

    def fake_request(method, path, *, query=None, json_body=None, account=False):
        seen["query"] = query
        return {"quotes": [{"symbol": "AAPL", "outcome": "OK", "price": "1", "requestedAt": "2026-09-10T23:10:00+09:00"}]}

    monkeypatch.setattr(client, "_request", fake_request)
    assert client.get_quotes(["US:AAPL"])[0].symbol == "US:AAPL"
    assert seen["query"] == {"symbols": "AAPL"}
    with pytest.raises(ValueError):
        client.get_quotes(["KRX:005930"])


def test_kis_environment_host_and_tr_prefix():
    from hermetix import TradingEnvironment
    paper = KisClient("k", "s", "50199202")
    live = KisClient("k", "s", "50199202", environment=TradingEnvironment.LIVE)
    assert paper._http.base_url == KisClient.PAPER_URL and paper._tr("TTC0802U") == "VTTC0802U"
    assert live._http.base_url == KisClient.LIVE_URL and live._tr("TTC0802U") == "TTTC0802U"
    assert paper._throttle.min_interval == 0.6 and live._throttle.min_interval == 0.1
    assert KisClient("k", "s", "c", base_url="http://custom")._http.base_url == "http://custom"
    assert KiwoomClient("k", "s", environment=TradingEnvironment.LIVE)._http.base_url == KiwoomClient.LIVE_URL
    assert KiwoomClient("k", "s")._http.base_url == KiwoomClient.PAPER_URL
