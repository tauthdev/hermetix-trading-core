package hermetix

// DB증권 스트림 — 가짜 웹소켓 서버로 등록·ACK·체결·호가·통보·재접속 흐름과 픽스처(문서 기반) 파싱을 검증한다.

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type dbMsg struct {
	Header struct {
		Token  string `json:"token"`
		TrType string `json:"tr_type"`
	} `json:"header"`
	Body map[string]any `json:"body"`
}

func parseDbMsg(t *testing.T, raw string) dbMsg {
	t.Helper()
	var m dbMsg
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func newDbTestStream(t *testing.T) (*DbMarketStream, chan *serverConn) {
	t.Helper()
	srv, connections := newWsServer(t)
	stream := newDbMarketStream(wsURL(srv), func() (string, error) { return "TOKEN", nil })
	t.Cleanup(func() { stream.Close() })
	return stream, connections
}

func TestDbStreamSubscribeAndTick(t *testing.T) {
	fx := loadStreamFixture(t, "db")
	stream, connections := newDbTestStream(t)
	ticks := make(chan TradeTick, 16)
	stream.SubscribeTrades([]string{"KRX:005930"}, func(tick TradeTick) { ticks <- tick })
	stream.Connect()

	conn := waitConn(t, connections, 5*time.Second)
	msg := parseDbMsg(t, conn.take(t))
	if msg.Header.Token != "TOKEN" || msg.Header.TrType != "1" || msg.Body["tr_cd"] != "S00" || msg.Body["tr_key"] != "J 005930" {
		t.Fatalf("subscribe = %+v", msg)
	}
	conn.send(t, `{"header":{"tr_cd":"S00","tr_key":null},"body":{"tr_key":["J 005930"]}}`) // ack
	conn.send(t, `{"header":null,"body":null}`)                                             // keepalive 류
	conn.send(t, fx.Frames[0])
	tick := waitTick(t, ticks)
	if tick.Symbol != "KRX:005930" || tick.Price.String() != "143300" || tick.Quantity.String() != "100" {
		t.Fatalf("tick = %+v", tick)
	}
	if tick.CumulativeVolume == nil || *tick.CumulativeVolume != 405039 {
		t.Fatalf("cumulative = %v", tick.CumulativeVolume)
	}
	assertDecimalEqual(t, "change", tick.Change, "33000")
	assertDecimalEqual(t, "changeRate", tick.ChangeRate, "0.2992")
	assertDecimalEqual(t, "ask", tick.AskPrice, "140200")
	assertDecimalEqual(t, "bid", tick.BidPrice, "143300")
}

func TestDbStreamRegistrationsAndOrderEvents(t *testing.T) {
	fx := loadStreamFixture(t, "db")
	stream, connections := newDbTestStream(t)
	books := make(chan OrderBookTick, 4)
	events := make(chan OrderEvent, 8)
	if err := stream.SubscribeOrderBook([]string{"005930"}, func(b OrderBookTick) { books <- b }); err != nil {
		t.Fatal(err)
	}
	if err := stream.SubscribeOrderEvents(func(e OrderEvent) { events <- e }); err != nil {
		t.Fatal(err)
	}
	stream.Connect()
	conn := waitConn(t, connections, 5*time.Second)
	seen := map[string]dbMsg{}
	for i := 0; i < 3; i++ {
		m := parseDbMsg(t, conn.take(t))
		seen[m.Body["tr_cd"].(string)] = m
	}
	if seen["S01"].Header.TrType != "1" || seen["S01"].Body["tr_key"] != "J 005930" {
		t.Fatalf("S01 = %+v", seen["S01"])
	}
	for _, tr := range []string{"IS0", "IS1"} {
		m, ok := seen[tr]
		if !ok || m.Header.TrType != "3" {
			t.Fatalf("%s 등록은 tr_type 3 이어야 한다: %+v", tr, m)
		}
		if _, has := m.Body["tr_key"]; has {
			t.Fatalf("%s 등록에는 tr_key 가 없어야 한다: %+v", tr, m)
		}
	}

	conn.send(t, fx.OrderBook.Frames[0])
	var book OrderBookTick
	select {
	case book = <-books:
	case <-time.After(5 * time.Second):
		t.Fatal("호가가 오지 않음")
	}
	best, _ := book.BestAsk()
	if book.Symbol != "005930" || len(book.Asks) != 10 || best.Price.String() != "54500" {
		t.Fatalf("book = %+v", book)
	}

	for _, frame := range fx.OrderEvents.Frames {
		conn.send(t, frame)
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
	accepted := take()
	if accepted.Type != OrderAccepted || !accepted.OrderIDMatches("34048") || accepted.Symbol != "005930" || *accepted.Side != Buy {
		t.Fatalf("accepted = %+v", accepted)
	}
	assertDecimalEqual(t, "quantity", accepted.Quantity, "10")
	assertDecimalEqual(t, "price", accepted.Price, "80000")
	if filled := take(); filled.Type != OrderFilled {
		t.Fatalf("filled = %+v", filled)
	}
	canceled := take()
	if canceled.Type != OrderCanceled || !canceled.OrderIDMatches("241048") || canceled.OriginalOrderID != "0000241038" || canceled.Symbol != "004410" {
		t.Fatalf("canceled = %+v", canceled)
	}

	// 재접속 시 S01·IS0·IS1 을 다시 보낸다
	_ = conn.conn.Close(1000, "bye")
	second := waitConn(t, connections, 10*time.Second)
	again := map[string]bool{}
	for i := 0; i < 3; i++ {
		again[parseDbMsg(t, second.take(t)).Body["tr_cd"].(string)] = true
	}
	if !again["S01"] || !again["IS0"] || !again["IS1"] {
		t.Fatalf("resend = %v", again)
	}
}

func TestDbParsersEdgeCases(t *testing.T) {
	today := fixtureToday
	// 소문자 키·하락 부호·0 호가 단계
	body := map[string]any{"shrniscd": "A005930", "stckcntghour": "101010", "stckprpr": "1000", "cntgvol": "3", "acmlvol": "10", "prdyvrss": "50", "prdyvrssclr": "-", "prdyctrt": "-4.76", "askp1": "1010", "bidp1": "990"}
	tick, ok := ParseDbTrade(body, today)
	if !ok || tick.Symbol != "005930" {
		t.Fatalf("tick = %+v", tick)
	}
	assertDecimalEqual(t, "change", tick.Change, "-50")
	assertDecimalEqual(t, "ask", tick.AskPrice, "1010")

	book := map[string]any{"ShrnIscd": "005930", "BsopHour": "101010", "Askp1": "1010", "AskpRsqn1": "5", "Askp2": "0", "Bidp1": "990", "BidpRsqn1": "7", "TotalAskprsqn": "5", "TotalBidprsqn": "7"}
	ob, ok := ParseDbOrderBook(book, today)
	if !ok || len(ob.Asks) != 1 || len(ob.Bids) != 1 {
		t.Fatalf("book = %+v", ob)
	}

	rejected := map[string]any{"Sordno": "0000000009", "Sorgordno": "0000000000", "Sshtnisuno": "A005930", "Sbnstp": "2", "Sordqty": "1", "Sordprc": "1000", "Srjtqty": "1", "Sexecqty": "0", "Scanccnfqty": "0", "Smdfycnfqty": "0", "Sunercqty": "0", "Sexectime": "101010999"}
	ev, ok := ParseDbOrderEvent("IS1", rejected, today)
	if !ok || ev.Type != OrderRejected || ev.OriginalOrderID != "" {
		t.Fatalf("rejected = %+v", ev)
	}
	modified := map[string]any{"Sordno": "0000000010", "Sorgordno": "0000000009", "Sshtnisuno": "A005930", "Sbnstp": "2", "Sordqty": "1", "Sordprc": "1000", "Srjtqty": "0", "Sexecqty": "0", "Scanccnfqty": "0", "Smdfycnfqty": "1", "Smdfycnfprc": "1100", "Sunercqty": "1", "Sexectime": "101011000"}
	ev, ok = ParseDbOrderEvent("IS1", modified, today)
	if !ok || ev.Type != OrderModified || ev.OriginalOrderID != "0000000009" || ev.Price.String() != "1100" {
		t.Fatalf("modified = %+v", ev)
	}
	if _, ok := ParseDbOrderEvent("IS0", map[string]any{"Sordqty": "1"}, today); ok {
		t.Fatal("주문번호가 없으면 false 여야 한다")
	}
	if !strings.HasPrefix(DbNormalizeStreamCode("U-005930"), "005930") || DbNormalizeStreamCode("N-005930") != "005930" {
		t.Fatal("접두 제거")
	}
}

func TestDbStreamFixture(t *testing.T) {
	fx := loadStreamFixture(t, "db")
	if fx.Measured {
		t.Fatal("db 픽스처는 문서 기반(measured=false)이어야 한다")
	}
	var ticks []TradeTick
	for _, raw := range fx.Frames {
		if tick, ok := ParseDbTrade(parseFrame(t, raw).Body, fixtureToday); ok {
			ticks = append(ticks, tick)
		}
	}
	assertStreamTicks(t, ticks, fx)

	var books []OrderBookTick
	for _, raw := range fx.OrderBook.Frames {
		if book, ok := ParseDbOrderBook(parseFrame(t, raw).Body, fixtureToday); ok {
			books = append(books, book)
		}
	}
	assertBooks(t, books, fx.OrderBook)

	var events []OrderEvent
	for _, raw := range fx.OrderEvents.Frames {
		frame := parseFrame(t, raw)
		if ev, ok := ParseDbOrderEvent(frameText(frame.Header, "tr_cd"), frame.Body, fixtureToday); ok {
			events = append(events, ev)
		}
	}
	assertOrderEvents(t, events, fx.OrderEvents)
}
