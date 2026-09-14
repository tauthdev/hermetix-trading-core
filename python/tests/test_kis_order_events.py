"""주문 통보가 KIS 메모리 주문 추적에 반영되는지 - 모의 서버는 주문 조회가 없어 통보가 유일한 즉시 확정 경로 (Kotlin KisOrderEventTest)."""
from datetime import datetime, timezone
from decimal import Decimal

from hermetix import CreateOrderRequest, KisClient, OrderEvent, OrderEventType, OrderSide, OrderStatus, OrderType


class FakeHttp:
    """tr_id / path 로 라우팅 - 큐 방식은 순서가 어긋나면 요청이 무한 대기한다."""

    def __init__(self):
        self.balance_calls = 0

    def request(self, method, path, *, headers=None, query=None, json_body=None, form_body=None):
        headers = headers or {}
        if path.startswith("/oauth2/tokenP"):
            return 200, {"access_token": "tok", "expires_in": 86400}
        if headers.get("tr_id") == "VTTC0802U":
            return 200, {"rt_cd": "0", "msg_cd": "APBK0013", "msg1": "주문 전송 완료",
                         "output": {"KRX_FWDG_ORD_ORGNO": "00950", "ODNO": "0012345", "ORD_TMD": "105530"}}
        if headers.get("tr_id") == "VTTC8434R":
            self.balance_calls += 1
            return 200, {"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok", "output1": [],
                         "output2": [{"dnca_tot_amt": "1000000", "tot_evlu_amt": "1000000"}]}
        return 599, {"rt_cd": "1", "msg1": f"unexpected {path} {headers.get('tr_id')}"}


def client():
    c = KisClient("k", "s", "50199202", throttle_seconds=0.001)
    c._http = FakeHttp()
    return c


def event(kind, quantity=None, price=None):
    return OrderEvent(order_id="0000012345", type=kind, timestamp=datetime.now(timezone.utc), symbol="005930",
                      side=OrderSide.BUY, quantity=Decimal(quantity) if quantity else None,
                      price=Decimal(price) if price else None)


def buy(c):
    return c.create_order(CreateOrderRequest(symbol="005930", side=OrderSide.BUY, order_type=OrderType.LIMIT,
                                             quantity=Decimal(2), limit_price=Decimal(250000)))


def test_fill_events_accumulate_to_filled_and_skip_holdings_refresh_afterwards():
    c = client()
    assert buy(c).order_id == "0012345"
    baseline = c._http.balance_calls  # create_order 가 기준 보유를 1회 조회

    c.apply_order_event(event(OrderEventType.ACCEPTED, "2", "250000"))
    c.apply_order_event(event(OrderEventType.FILLED, "1", "250000"))
    assert c.get_order("0012345").status == OrderStatus.PARTIALLY_FILLED  # 아직 open -> refresh 1회

    c.apply_order_event(event(OrderEventType.FILLED, "1", "250500"))
    filled = c.get_order("0012345")
    assert filled.status == OrderStatus.FILLED
    assert filled.filled_quantity == 2 and filled.avg_fill_price == 250500
    assert c._http.balance_calls == baseline + 1
    assert len(c.get_fills()) == 1
    assert c._http.balance_calls == baseline + 1  # FILLED 뒤에는 조회 없음


def test_cancel_event_and_unknown_order_id():
    c = client()
    buy(c)
    c.apply_order_event(event(OrderEventType.CANCELED))
    assert c.get_order("0012345").status == OrderStatus.CANCELED
    c.apply_order_event(OrderEvent(order_id="9999999", type=OrderEventType.FILLED, timestamp=datetime.now(timezone.utc),
                                   quantity=Decimal(1)))
    assert c.get_orders() == []
