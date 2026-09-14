package hermetix

// NH PLUG 스트림 — 가짜 웹소켓 서버로 구독·ACK·체결·호가·통보·재접속 흐름과 픽스처(문서 기반) 파싱을 검증한다.

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type nhMsg struct {
	Header struct {
		Token  string `json:"token"`
		TrType string `json:"tr_type"`
	} `json:"header"`
	Body struct {
		TrCd  string `json:"tr_cd"`
		TrKey string `json:"tr_key"`
	} `json:"body"`
}

func parseNhMsg(t *testing.T, raw string) nhMsg {
	t.Helper()
	var m nhMsg
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// parseFrame - 매번 새 jsonFrame 으로 파싱한다 (json.Unmarshal 은 기존 맵에 키를 합치므로 변수 재사용 금지).
func parseFrame(t *testing.T, raw string) jsonFrame {
	t.Helper()
	var frame jsonFrame
	if err := json.Unmarshal([]byte(raw), &frame); err != nil {
		t.Fatal(err)
	}
	return frame
}

func newNhTestStream(t *testing.T, marketCd string) (*NhMarketStream, chan *serverConn) {
	t.Helper()
	srv, connections := newWsServer(t)
	stream := newNhMarketStream(wsURL(srv), marketCd, "", func() (string, error) { return "TOKEN", nil })
	t.Cleanup(func() { stream.Close() })
	return stream, connections
}

func TestNhStreamSubscribeAndTick(t *testing.T) {
	fx := loadStreamFixture(t, "nh")
	stream, connections := newNhTestStream(t, "UNT")
	ticks := make(chan TradeTick, 16)
	stream.SubscribeTrades([]string{"KRX:005940"}, func(tick TradeTick) { ticks <- tick })
	stream.Connect()

	conn := waitConn(t, connections, 5*time.Second)
	msg := parseNhMsg(t, conn.take(t))
	if msg.Header.Token != "TOKEN" || msg.Header.TrType != "1" || msg.Body.TrCd != "mc" || msg.Body.TrKey != "005940" {
		t.Fatalf("subscribe = %+v", msg)
	}
	conn.send(t, `{"header":{"tr_type":"1","tr_cd":"mc","rsp_cd":"00000","rsp_msg":"정상처리되었습니다"},"body":{"tr_key":["005940"]}}`)
	conn.send(t, fx.Frames[0])
	tick := waitTick(t, ticks)
	if tick.Symbol != "KRX:005940" || tick.Price.String() != "31750" || tick.Quantity.String() != "13" {
		t.Fatalf("tick = %+v", tick)
	}
	if tick.CumulativeVolume == nil || *tick.CumulativeVolume != 837624 {
		t.Fatalf("cumulative = %v", tick.CumulativeVolume)
	}
	assertDecimalEqual(t, "change", tick.Change, "2500")
	assertDecimalEqual(t, "changeRate", tick.ChangeRate, "0.0855")
	assertDecimalEqual(t, "ask", tick.AskPrice, "31750")
	assertDecimalEqual(t, "bid", tick.BidPrice, "31700")
	if !stream.IsConnected() {
		t.Fatal("connected 여야 한다")
	}
}

func TestNhStreamKrxChannels(t *testing.T) {
	stream, connections := newNhTestStream(t, "KRX")
	stream.SubscribeTrades([]string{"005930"}, func(TradeTick) {})
	if err := stream.SubscribeOrderBook([]string{"005930"}, func(OrderBookTick) {}); err != nil {
		t.Fatal(err)
	}
	stream.Connect()
	conn := waitConn(t, connections, 5*time.Second)
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		m := parseNhMsg(t, conn.take(t))
		got[m.Body.TrCd] = true
	}
	if !got["oc"] || !got["ob"] {
		t.Fatalf("KRX 채널은 oc/ob 여야 한다: %v", got)
	}
}

func TestNhStreamOrderBook(t *testing.T) {
	fx := loadStreamFixture(t, "nh")
	stream, connections := newNhTestStream(t, "UNT")
	books := make(chan OrderBookTick, 4)
	if err := stream.SubscribeOrderBook([]string{"KRX:005940"}, func(b OrderBookTick) { books <- b }); err != nil {
		t.Fatal(err)
	}
	stream.Connect()
	conn := waitConn(t, connections, 5*time.Second)
	if m := parseNhMsg(t, conn.take(t)); m.Body.TrCd != "mb" {
		t.Fatalf("book subscribe = %+v", m)
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
	if book.Symbol != "KRX:005940" || len(book.Asks) != 10 || len(book.Bids) != 10 ||
		best.Price.String() != "31800" || best.Quantity.String() != "668" || bid.Price.String() != "31750" || bid.Quantity.String() != "2899" {
		t.Fatalf("book = %+v", book)
	}
	assertDecimalEqual(t, "totalAsk", book.TotalAskQuantity, "33938")
	assertDecimalEqual(t, "totalBid", book.TotalBidQuantity, "13132")
}

func TestNhStreamOrderEventsAndReconnect(t *testing.T) {
	fx := loadStreamFixture(t, "nh")
	stream, connections := newNhTestStream(t, "UNT")
	events := make(chan OrderEvent, 8)
	stream.SubscribeTrades([]string{"005940"}, func(TradeTick) {})
	if err := stream.SubscribeOrderEvents(func(e OrderEvent) { events <- e }); err != nil {
		t.Fatal(err)
	}
	stream.Connect()
	conn := waitConn(t, connections, 5*time.Second)
	registered := map[string]string{}
	for i := 0; i < 3; i++ {
		m := parseNhMsg(t, conn.take(t))
		registered[m.Body.TrCd] = m.Body.TrKey
	}
	if _, ok := registered["mc"]; !ok || registered["d2"] != "" || registered["d3"] != "" {
		t.Fatalf("registrations = %v", registered)
	}
	if v, ok := registered["d2"]; !ok || v != "" {
		t.Fatalf("d2 tr_key 는 빈 문자열이어야 한다: %v", registered)
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
	if accepted.Type != OrderAccepted || !accepted.OrderIDMatches("30") || accepted.Symbol != "005940" || accepted.Side == nil || *accepted.Side != Buy {
		t.Fatalf("accepted = %+v", accepted)
	}
	assertDecimalEqual(t, "quantity", accepted.Quantity, "10")
	assertDecimalEqual(t, "price", accepted.Price, "35550")
	filled := take()
	if filled.Type != OrderFilled {
		t.Fatalf("filled = %+v", filled)
	}
	assertDecimalEqual(t, "fill quantity", filled.Quantity, "5")

	// 서버가 끊으면 재접속해 세 등록을 다시 보낸다
	_ = conn.conn.Close(1000, "bye")
	second := waitConn(t, connections, 10*time.Second)
	again := map[string]bool{}
	for i := 0; i < 3; i++ {
		again[parseNhMsg(t, second.take(t)).Body.TrCd] = true
	}
	if !again["mc"] || !again["d2"] || !again["d3"] {
		t.Fatalf("resubscribe = %v", again)
	}
}

func TestNhParsersEdgeCases(t *testing.T) {
	today := fixtureToday
	numeric := `{"header":{"tr_cd":"mc","tr_key":"005930"},"body":{"code":"005930","time":"093012","sign":5,"change":300,"price":71500,"chrate":0.42,"offer":71500,"bid":71400,"movolume":15,"new_volume":1234567}}`
	ticks := ParseNhTrade(parseFrame(t, numeric), today)
	if len(ticks) != 1 || ticks[0].Price.String() != "71500" || ticks[0].Quantity.String() != "15" {
		t.Fatalf("numeric ticks = %+v", ticks)
	}
	assertDecimalEqual(t, "change (falling)", ticks[0].Change, "-300")
	assertDecimalEqual(t, "changeRate (falling)", ticks[0].ChangeRate, "-0.0042")
	if got := ticks[0].Timestamp.In(kst).Format("15:04:05"); got != "09:30:12" {
		t.Fatalf("time = %s", got)
	}

	// itemgb 가 2(파생)이면 무시, 계좌 필터 불일치도 무시
	d2 := `{"header":{"tr_cd":"d2"},"body":{"itemgb":"2","accountno":"A","orderno":"1","issuecd":"005930","slbygb":"2","concgty":"1","concprc":"1","conctime":"090000","ucgb":"0","rejgb":"0"}}`
	if len(ParseNhOrderEvents(parseFrame(t, d2), today, "")) != 0 {
		t.Fatal("itemgb=2 는 걸러져야 한다")
	}
	d2 = strings.Replace(d2, `"itemgb":"2"`, `"itemgb":"1"`, 1)
	if len(ParseNhOrderEvents(parseFrame(t, d2), today, "B")) != 0 {
		t.Fatal("계좌가 다르면 걸러져야 한다")
	}
	if events := ParseNhOrderEvents(parseFrame(t, d2), today, "A"); len(events) != 1 || events[0].Type != OrderFilled {
		t.Fatalf("filled = %+v", events)
	}
	cancel := strings.Replace(d2, `"ucgb":"0"`, `"ucgb":"2"`, 1)
	if events := ParseNhOrderEvents(parseFrame(t, cancel), today, ""); len(events) != 1 || events[0].Type != OrderCanceled {
		t.Fatalf("canceled = %+v", events)
	}
	reject := strings.Replace(d2, `"rejgb":"0"`, `"rejgb":"1"`, 1)
	if events := ParseNhOrderEvents(parseFrame(t, reject), today, ""); len(events) != 1 || events[0].Type != OrderRejected {
		t.Fatalf("rejected = %+v", events)
	}
	modifyAccept := `{"header":{"tr_cd":"d3"},"body":{"itemgb":"1","orderno":"0000000031","orgordno":"0000000030","issuecd":"005930","slbygb":"1","ordergty":"3","orderprc":"70000","order_time":"100000"}}`
	if events := ParseNhOrderEvents(parseFrame(t, modifyAccept), today, ""); len(events) != 1 || events[0].Type != OrderAccepted || events[0].OriginalOrderID != "0000000030" || *events[0].Side != Sell {
		t.Fatalf("modify accept = %+v", events)
	}
}

func TestNhStreamFixture(t *testing.T) {
	fx := loadStreamFixture(t, "nh")
	if fx.Measured {
		t.Fatal("nh 픽스처는 문서 기반(measured=false)이어야 한다")
	}
	var ticks []TradeTick
	for _, raw := range fx.Frames {
		ticks = append(ticks, ParseNhTrade(parseFrame(t, raw), fixtureToday)...)
	}
	assertStreamTicks(t, ticks, fx)

	var books []OrderBookTick
	for _, raw := range fx.OrderBook.Frames {
		books = append(books, ParseNhOrderBook(parseFrame(t, raw), fixtureToday)...)
	}
	assertBooks(t, books, fx.OrderBook)

	var events []OrderEvent
	for _, raw := range fx.OrderEvents.Frames {
		events = append(events, ParseNhOrderEvents(parseFrame(t, raw), fixtureToday, "")...)
	}
	assertOrderEvents(t, events, fx.OrderEvents)
}
