package hermetix

// 토스증권 스트림 — Bearer 핸드셰이크, 선언형 구독(합치기·거부 제외), 체결·호가·주문 이벤트(누적 델타), PING 하트비트, 재접속, 픽스처(AsyncAPI 샘플) 파싱.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type tossServerConn struct {
	*serverConn
	authorization string
}

// newTossWsServer - 업그레이드 요청의 Authorization 헤더를 함께 기록한다.
func newTossWsServer(t *testing.T) (*httptest.Server, chan *tossServerConn) {
	t.Helper()
	connections := make(chan *tossServerConn, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		sc := &tossServerConn{serverConn: &serverConn{conn: conn, received: make(chan string, 64)}, authorization: auth}
		connections <- sc
		for {
			kind, data, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			if kind == websocket.MessageText {
				sc.received <- string(data)
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv, connections
}

func waitTossConn(t *testing.T, connections chan *tossServerConn) *tossServerConn {
	t.Helper()
	select {
	case c := <-connections:
		return c
	case <-time.After(10 * time.Second):
		t.Fatal("접속이 오지 않음")
		return nil
	}
}

func newTossTestStream(t *testing.T) (*TossMarketStream, chan *tossServerConn) {
	t.Helper()
	srv, connections := newTossWsServer(t)
	stream := newTossMarketStream(wsURL(srv), func() (string, error) { return "ACCESS", nil }, func() (string, error) { return "3", nil })
	stream.declareDelay = 50 * time.Millisecond
	t.Cleanup(func() { stream.Close() })
	return stream, connections
}

type tossDeclaration []map[string]any

func parseDeclaration(t *testing.T, raw string) tossDeclaration {
	t.Helper()
	var d tossDeclaration
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatalf("declaration is not a JSON array: %s", raw)
	}
	return d
}

func (d tossDeclaration) codes(typ string) []string {
	for _, item := range d {
		if item["type"] == typ {
			raw, _ := item["codes"].([]any)
			out := make([]string, 0, len(raw))
			for _, c := range raw {
				out = append(out, c.(string))
			}
			return out
		}
	}
	return nil
}

func TestTossStreamBearerAndDeclaration(t *testing.T) {
	stream, connections := newTossTestStream(t)
	stream.SubscribeTrades([]string{"KRX:005930", "US:AAPL"}, func(TradeTick) {})
	_ = stream.SubscribeOrderBook([]string{"KRX:005930"}, func(OrderBookTick) {})
	_ = stream.SubscribeOrderEvents(func(OrderEvent) {})
	stream.Connect()

	conn := waitTossConn(t, connections)
	if conn.authorization != "Bearer ACCESS" {
		t.Fatalf("Authorization = %q", conn.authorization)
	}
	decl := parseDeclaration(t, conn.take(t))
	if decl[0]["id"] == nil || len(decl) != 5 {
		t.Fatalf("declaration = %v", decl)
	}
	if got := decl.codes("trade:kr"); len(got) != 1 || got[0] != "005930" {
		t.Fatalf("trade:kr = %v", got)
	}
	if got := decl.codes("trade:us"); len(got) != 1 || got[0] != "AAPL" {
		t.Fatalf("trade:us = %v", got)
	}
	if got := decl.codes("orderbook:kr"); len(got) != 1 || got[0] != "005930" {
		t.Fatalf("orderbook:kr = %v", got)
	}
	if got := decl.codes("personal:order"); len(got) != 1 || got[0] != "3" {
		t.Fatalf("personal:order = %v", got)
	}

	// 거부된 target 은 다음 선언에서 빠진다 (늦은 구독이 재선언을 유발)
	conn.send(t, `{"type":"subscriptions","id":"req-1","subscribed":["trade:kr:005930"],"rejected":[{"target":"trade:us:AAPL","code":"stock-not-found","message":"x"}]}`)
	time.Sleep(100 * time.Millisecond)
	stream.SubscribeTrades([]string{"000660"}, func(TradeTick) {})
	next := parseDeclaration(t, conn.take(t))
	if got := next.codes("trade:us"); len(got) != 0 {
		t.Fatalf("거부된 AAPL 이 다시 선언됐다: %v", got)
	}
	if got := next.codes("trade:kr"); len(got) != 2 || got[0] != "000660" || got[1] != "005930" {
		t.Fatalf("trade:kr = %v", got)
	}
}

func TestTossStreamDataFrames(t *testing.T) {
	fx := loadStreamFixture(t, "toss")
	stream, connections := newTossTestStream(t)
	ticks := make(chan TradeTick, 8)
	books := make(chan OrderBookTick, 8)
	events := make(chan OrderEvent, 8)
	stream.SubscribeTrades([]string{"US:AAPL", "KRX:005930", "000660"}, func(tick TradeTick) { ticks <- tick })
	_ = stream.SubscribeOrderBook([]string{"KRX:005930"}, func(b OrderBookTick) { books <- b })
	_ = stream.SubscribeOrderEvents(func(e OrderEvent) { events <- e })
	stream.Connect()
	conn := waitTossConn(t, connections)
	conn.take(t) // 선언

	conn.send(t, `{"type":"pong"}`)
	conn.send(t, `{"type":"error","error":{"code":"rate-limit-exceeded","message":"slow down"}}`)
	conn.send(t, fx.Frames[0])
	tick := waitTick(t, ticks)
	if tick.Symbol != "US:AAPL" || tick.Price.String() != "243.26" || tick.Quantity.String() != "8" || tick.CumulativeVolume != nil || tick.Change != nil {
		t.Fatalf("tick = %+v", tick)
	}
	if got := tick.Timestamp.In(kst).Format("15:04"); got != "23:30" {
		t.Fatalf("time = %s", got)
	}
	// 접두 없이 구독한 심볼은 접두 없이 돌려준다 (요청 표기는 채널 공통 — 같은 종목을 다른 표기로 두 번 구독하면 첫 표기를 쓴다)
	conn.send(t, `{"type":"message","topic":"trade:kr:000660","data":{"price":"71500","volume":"3","timestamp":"2026-09-14T10:00:00.000+09:00","currency":"KRW"}}`)
	if bare := waitTick(t, ticks); bare.Symbol != "000660" {
		t.Fatalf("bare tick = %+v", bare)
	}

	conn.send(t, fx.OrderBook.Frames[0])
	var book OrderBookTick
	select {
	case book = <-books:
	case <-time.After(5 * time.Second):
		t.Fatal("호가가 오지 않음")
	}
	best, _ := book.BestAsk()
	bid, _ := book.BestBid()
	if book.Symbol != "KRX:005930" || best.Price.String() != "71500" || best.Quantity.String() != "5" || bid.Price.String() != "71400" || book.TotalAskQuantity != nil {
		t.Fatalf("book = %+v", book)
	}

	take := func() OrderEvent {
		select {
		case e := <-events:
			return e
		case <-time.After(5 * time.Second):
			t.Fatal("이벤트가 오지 않음")
			return OrderEvent{}
		}
	}
	order := `{"orderId":"ORD-1","symbol":"AAPL","side":"BUY","orderType":"LIMIT","timeInForce":"DAY","status":"%s","price":"100.5","quantity":"10","currency":"USD","orderedAt":"2026-06-23T09:30:00.000+09:00","canceledAt":null,"execution":{"filledQuantity":"%s","averageFilledPrice":"100","filledAmount":null,"commission":null,"tax":null,"settlementDate":null}}`
	frame := func(event, status, filled string) string {
		return `{"type":"message","topic":"personal:order:3","data":{"event":"` + event + `","accountSeq":"3","order":` + fmt.Sprintf(order, status, filled) + `}}`
	}
	conn.send(t, frame("PENDING", "PENDING", "0"))
	if accepted := take(); accepted.Type != OrderAccepted || accepted.Symbol != "US:AAPL" || *accepted.Side != Buy || accepted.Quantity.String() != "10" {
		t.Fatalf("accepted = %+v", accepted)
	}
	conn.send(t, frame("CANCELING", "PENDING_CANCEL", "0")) // 중간 상태 — 무시
	conn.send(t, frame("PARTIAL_FILL", "PARTIAL_FILLED", "4"))
	partial := take()
	if partial.Type != OrderFilled || partial.Quantity.String() != "4" || partial.Price.String() != "100" || partial.RemainingQuantity.String() != "6" {
		t.Fatalf("partial = %+v", partial)
	}
	conn.send(t, frame("FILL", "FILLED", "10"))
	full := take()
	if full.Type != OrderFilled || full.Quantity.String() != "6" || full.RemainingQuantity.String() != "0" {
		t.Fatalf("full = %+v", full)
	}
	conn.send(t, frame("CANCEL_REJECTED", "CANCEL_REJECTED", "10"))
	if rejected := take(); rejected.Type != OrderRejected || rejected.Reason != "CANCEL_REJECTED" {
		t.Fatalf("rejected = %+v", rejected)
	}
}

func TestTossStreamHeartbeatAndReconnect(t *testing.T) {
	srv, connections := newTossWsServer(t)
	stream := newTossMarketStream(wsURL(srv), func() (string, error) { return "ACCESS", nil }, func() (string, error) { return "3", nil })
	stream.declareDelay = 50 * time.Millisecond
	stream.reconnectingWebSocket.heartbeat = 150 * time.Millisecond
	defer stream.Close()
	stream.SubscribeTrades([]string{"KRX:005930"}, func(TradeTick) {})
	stream.Connect()

	conn := waitTossConn(t, connections)
	parseDeclaration(t, conn.take(t))
	if ping := conn.take(t); ping != "PING" {
		t.Fatalf("heartbeat = %q", ping)
	}

	_ = conn.conn.Close(1000, "server-shutdown")
	second := waitTossConn(t, connections)
	if second.authorization != "Bearer ACCESS" {
		t.Fatalf("재접속 Authorization = %q", second.authorization)
	}
	if again := parseDeclaration(t, second.take(t)); len(again.codes("trade:kr")) != 1 {
		t.Fatalf("재선언 = %v", again)
	}
}

func TestTossTopicKey(t *testing.T) {
	for symbol, want := range map[string]string{"005930": "kr:005930", "KRX:005930": "kr:005930", "US:aapl": "us:AAPL"} {
		if got, err := TossTopicKey(symbol); err != nil || got != want {
			t.Fatalf("%s → %s (%v), expected %s", symbol, got, err, want)
		}
	}
	if _, err := TossTopicKey("JP:7203"); err == nil {
		t.Fatal("미지원 시장은 오류여야 한다")
	}
}

func TestTossStreamFixture(t *testing.T) {
	fx := loadStreamFixture(t, "toss")
	if fx.Measured {
		t.Fatal("toss 픽스처는 문서 기반(measured=false)이어야 한다")
	}
	// 체결: 누적거래량·등락이 없어(null) 공통 assertStreamTicks 대신 필드별 확인
	var frame struct {
		Topic string          `json:"topic"`
		Data  json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(fx.Frames[0]), &frame); err != nil {
		t.Fatal(err)
	}
	tick, ok := ParseTossTrade(frame.Topic, frame.Data)
	e := fx.Expected[0]
	if !ok || tick.Symbol != e.Symbol || tick.Price.String() != e.Price || tick.Quantity.String() != e.Quantity || tick.CumulativeVolume != nil || tick.Change != nil || tick.ChangeRate != nil {
		t.Fatalf("tick = %+v, expected %+v", tick, e)
	}
	if got := tick.Timestamp.In(kst).Format("15:04"); got != e.Time {
		t.Fatalf("time = %s, expected %s", got, e.Time)
	}

	if err := json.Unmarshal([]byte(fx.OrderBook.Frames[0]), &frame); err != nil {
		t.Fatal(err)
	}
	book, ok := ParseTossOrderBook(frame.Topic, frame.Data)
	be := fx.OrderBook.Expected[0]
	if !ok || book.Symbol != be.Symbol || len(book.Asks) != len(be.Asks) || len(book.Bids) != len(be.Bids) ||
		book.Asks[0].Price.String() != be.Asks[0].Price || book.Asks[0].Quantity.String() != be.Asks[0].Quantity ||
		book.Bids[0].Price.String() != be.Bids[0].Price || book.TotalAskQuantity != nil {
		t.Fatalf("book = %+v, expected %+v", book, be)
	}
	if got := book.Timestamp.In(kst).Format("15:04"); got != be.Time {
		t.Fatalf("book time = %s, expected %s", got, be.Time)
	}

	if err := json.Unmarshal([]byte(fx.OrderEvents.Frames[0]), &frame); err != nil {
		t.Fatal(err)
	}
	event, ok := ParseTossOrderEvent(frame.Data, nil)
	ee := fx.OrderEvents.Expected[0]
	if !ok || event.OrderID != ee.OrderID || string(event.Type) != ee.Type || event.Symbol != ee.Symbol || event.Side == nil || string(*event.Side) != ee.Side {
		t.Fatalf("event = %+v, expected %+v", event, ee)
	}
	assertDecimalEqual(t, "quantity", event.Quantity, ee.Quantity)
	assertDecimalEqual(t, "price", event.Price, ee.Price)
	assertDecimalEqual(t, "remaining", event.RemainingQuantity, ee.RemainingQuantity)
	if got := event.Timestamp.In(kst).Format("15:04"); got != ee.Time {
		t.Fatalf("event time = %s, expected %s", got, ee.Time)
	}
}
