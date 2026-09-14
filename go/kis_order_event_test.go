package hermetix

// 주문 통보가 KIS 메모리 주문 추적에 반영되는지 — 모의 서버는 주문 조회가 없어 통보가 유일한 즉시 확정 경로.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func newKisOrderEventClient(balanceCalls *atomic.Int32) *KisClient {
	c := NewKisClient("k", "s", "50199202")
	c.call = func(method, path, trID string, query, jsonBody map[string]string) (map[string]any, error) {
		if trID == "VTTC8434R" { // 잔고
			balanceCalls.Add(1)
			return map[string]any{
				"output1": []any{},
				"output2": []any{map[string]any{"dnca_tot_amt": "1000000", "tot_evlu_amt": "1000000"}},
			}, nil
		}
		return map[string]any{"output": map[string]any{"ODNO": "0012345"}}, nil
	}
	return c
}

func kisEvent(kind OrderEventType, quantity, price string) OrderEvent {
	side := Buy
	e := OrderEvent{OrderID: "0000012345", Type: kind, Timestamp: time.Now(), Symbol: "005930", Side: &side}
	if quantity != "" {
		q, _ := decimal.NewFromString(quantity)
		e.Quantity = &q
	}
	if price != "" {
		p, _ := decimal.NewFromString(price)
		e.Price = &p
	}
	return e
}

func TestKisApplyOrderEventFills(t *testing.T) {
	var balanceCalls atomic.Int32
	c := newKisOrderEventClient(&balanceCalls)
	limit := decimal.NewFromInt(250000)
	order, err := c.CreateOrder(CreateOrderRequest{Symbol: "005930", Side: Buy, OrderType: Limit, Quantity: decimal.NewFromInt(2), LimitPrice: &limit})
	if err != nil || order.OrderID != "0012345" {
		t.Fatalf("order = %+v err = %v", order, err)
	}
	baseline := balanceCalls.Load() // CreateOrder 가 기준 보유를 1회 조회

	c.ApplyOrderEvent(kisEvent(OrderAccepted, "2", "250000"))
	c.ApplyOrderEvent(kisEvent(OrderFilled, "1", "250000"))
	got, _ := c.GetOrder("0012345") // 아직 open → refresh 1회
	if got.Status != PartiallyFilled {
		t.Fatalf("status = %s, want PARTIALLY_FILLED", got.Status)
	}

	c.ApplyOrderEvent(kisEvent(OrderFilled, "1", "250500"))
	got, _ = c.GetOrder("0012345")
	if got.Status != Filled {
		t.Fatalf("status = %s, want FILLED", got.Status)
	}
	assertDecimalEqual(t, "filledQuantity", got.FilledQuantity, "2")
	assertDecimalEqual(t, "avgFillPrice", got.AvgFillPrice, "250500")
	if balanceCalls.Load() != baseline+1 {
		t.Fatalf("balance calls = %d, want %d (부분 체결 때의 refresh 1회뿐)", balanceCalls.Load(), baseline+1)
	}
	fills, _ := c.GetFills()
	if len(fills) != 1 {
		t.Fatalf("fills = %d", len(fills))
	}
	if balanceCalls.Load() != baseline+1 {
		t.Fatal("FILLED 뒤에는 보유 조회가 없어야 한다")
	}
}

func TestKisApplyOrderEventCancelAndUnknown(t *testing.T) {
	var balanceCalls atomic.Int32
	c := newKisOrderEventClient(&balanceCalls)
	limit := decimal.NewFromInt(250000)
	if _, err := c.CreateOrder(CreateOrderRequest{Symbol: "005930", Side: Buy, OrderType: Limit, Quantity: decimal.NewFromInt(2), LimitPrice: &limit}); err != nil {
		t.Fatal(err)
	}
	c.ApplyOrderEvent(kisEvent(OrderCanceled, "", ""))
	if got, _ := c.GetOrder("0012345"); got.Status != Canceled {
		t.Fatalf("status = %s, want CANCELED", got.Status)
	}
	one := decimal.NewFromInt(1)
	c.ApplyOrderEvent(OrderEvent{OrderID: "9999999", Type: OrderFilled, Timestamp: time.Now(), Quantity: &one})
	if orders, _ := c.GetOrders(); len(orders) != 0 {
		t.Fatalf("open orders = %d", len(orders))
	}
}
