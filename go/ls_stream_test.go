package hermetix

// LS증권 스트림 — 가짜 웹소켓 서버로 등록(KOSPI+KOSDAQ 이중)·ACK·체결·호가·통보·해제·재접속 흐름과 픽스처(문서 기반) 파싱을 검증한다.

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type lsMsg struct {
	Header struct {
		Token  string `json:"token"`
		TrType string `json:"tr_type"`
	} `json:"header"`
	Body struct {
		TrCd  string `json:"tr_cd"`
		TrKey string `json:"tr_key"`
	} `json:"body"`
}

func parseLsMsg(t *testing.T, raw string) lsMsg {
	t.Helper()
	var m lsMsg
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func newLsTestStream(t *testing.T) (*LsMarketStream, chan *serverConn) {
	t.Helper()
	srv, connections := newWsServer(t)
	stream := newLsMarketStream(wsURL(srv), func() (string, error) { return "TOKEN", nil })
	t.Cleanup(func() { stream.Close() })
	return stream, connections
}

func takeLs(t *testing.T, conn *serverConn, n int) map[string]lsMsg {
	t.Helper()
	out := map[string]lsMsg{}
	for i := 0; i < n; i++ {
		m := parseLsMsg(t, conn.take(t))
		out[m.Body.TrCd] = m
	}
	return out
}

func TestLsStreamDualSubscribeAndTick(t *testing.T) {
	fx := loadStreamFixture(t, "ls")
	stream, connections := newLsTestStream(t)
	ticks := make(chan TradeTick, 16)
	stream.SubscribeTrades([]string{"KRX:005930"}, func(tick TradeTick) { ticks <- tick })
	stream.Connect()

	conn := waitConn(t, connections, 5*time.Second)
	msgs := takeLs(t, conn, 2)
	for _, tr := range []string{"S3_", "K3_"} {
		m, ok := msgs[tr]
		if !ok || m.Header.Token != "TOKEN" || m.Header.TrType != "3" || m.Body.TrKey != "005930" {
			t.Fatalf("%s subscribe = %+v", tr, m)
		}
	}
	conn.send(t, `{"header":{"tr_cd":"S3_","tr_key":"005930","tr_type":"3","rsp_cd":"00000","rsp_msg":"정상처리되었습니다"}}`)
	conn.send(t, fx.Frames[0])
	tick := waitTick(t, ticks)
	if tick.Symbol != "KRX:005930" || tick.Price.String() != "55550" || tick.Quantity.String() != "1" {
		t.Fatalf("tick = %+v", tick)
	}
	if tick.CumulativeVolume == nil || *tick.CumulativeVolume != 10887 {
		t.Fatalf("cumulative = %v", tick.CumulativeVolume)
	}
	assertDecimalEqual(t, "change", tick.Change, "1050")
	assertDecimalEqual(t, "changeRate", tick.ChangeRate, "0.0193")
	assertDecimalEqual(t, "ask", tick.AskPrice, "55600")
	assertDecimalEqual(t, "bid", tick.BidPrice, "55500")

	// KOSDAQ TR 도 같은 레이아웃으로 전달된다
	conn.send(t, strings.Replace(fx.Frames[0], `"tr_cd":"S3_"`, `"tr_cd":"K3_"`, 1))
	if kosdaq := waitTick(t, ticks); kosdaq.Symbol != "KRX:005930" {
		t.Fatalf("kosdaq tick = %+v", kosdaq)
	}
}

func TestLsStreamOrderEventsBookAndReconnect(t *testing.T) {
	fx := loadStreamFixture(t, "ls")
	stream, connections := newLsTestStream(t)
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
	msgs := takeLs(t, conn, 7)
	for _, tr := range []string{"H1_", "HA_"} {
		if m := msgs[tr]; m.Header.TrType != "3" || m.Body.TrKey != "005930" {
			t.Fatalf("%s = %+v", tr, m)
		}
	}
	for _, tr := range lsOrderTRs {
		m, ok := msgs[tr]
		if !ok || m.Header.TrType != "1" || m.Body.TrKey != "" {
			t.Fatalf("%s 등록은 tr_type 1·tr_key 빈 문자열이어야 한다: %+v", tr, m)
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
	bid, _ := book.BestBid()
	if book.Symbol != "005930" || len(book.Asks) != 10 || best.Price.String() != "72400" || bid.Price.String() != "72300" {
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
	if accepted.Type != OrderAccepted || accepted.OrderID != "86382" || accepted.Symbol != "005930" || *accepted.Side != Buy {
		t.Fatalf("accepted = %+v", accepted)
	}
	assertDecimalEqual(t, "quantity", accepted.Quantity, "2")
	assertDecimalEqual(t, "price", accepted.Price, "60000")
	filled := take()
	if filled.Type != OrderFilled {
		t.Fatalf("filled = %+v", filled)
	}
	assertDecimalEqual(t, "fill qty", filled.Quantity, "1")
	assertDecimalEqual(t, "remaining", filled.RemainingQuantity, "1")
	canceled := take()
	if canceled.Type != OrderCanceled || canceled.OrderID != "88343" || canceled.OriginalOrderID != "88342" || canceled.Symbol != "000020" {
		t.Fatalf("canceled = %+v", canceled)
	}

	// 해제: 호가 tr_type 4, 계좌 tr_type 2
	stream.UnsubscribeOrderBook([]string{"005930"})
	stream.UnsubscribeOrderEvents()
	unsub := takeLs(t, conn, 7)
	if unsub["H1_"].Header.TrType != "4" || unsub["HA_"].Header.TrType != "4" || unsub["SC1"].Header.TrType != "2" {
		t.Fatalf("unsubscribe = %+v", unsub)
	}

	// 재접속: 남은 구독은 없으므로 아무것도 보내지 않아야 한다 — 새 트레이드 구독을 넣고 재접속을 확인
	stream.SubscribeTrades([]string{"000660"}, func(TradeTick) {})
	takeLs(t, conn, 2)
	_ = conn.conn.Close(1000, "bye")
	second := waitConn(t, connections, 10*time.Second)
	again := takeLs(t, second, 2)
	if again["S3_"].Body.TrKey != "000660" || again["K3_"].Body.TrKey != "000660" {
		t.Fatalf("resubscribe = %+v", again)
	}
}

func TestLsParsersEdgeCases(t *testing.T) {
	today := fixtureToday
	control := `{"header":{"tr_cd":"S3_","tr_key":"005930","rsp_cd":"00000","rsp_msg":"ok"}}`
	if _, ok := ParseLsTrade(parseFrame(t, control), today); ok {
		t.Fatal("제어 프레임은 파싱하지 않는다")
	}
	if len(ParseLsOrderEvents(parseFrame(t, control), today)) != 0 {
		t.Fatal("제어 프레임은 이벤트가 아니다")
	}
	falling := `{"header":{"tr_cd":"K3_","tr_key":"122870"},"body":{"chetime":"151055","sign":"5","change":"200","drate":"-0.37","price":"53900","cvolume":"438","volume":"14738346","offerho":"54000","bidho":"53900","shcode":"122870"}}`
	tick, ok := ParseLsTrade(parseFrame(t, falling), today)
	if !ok || tick.Symbol != "122870" {
		t.Fatalf("tick = %+v", tick)
	}
	assertDecimalEqual(t, "change (falling)", tick.Change, "-200")
	assertDecimalEqual(t, "changeRate", tick.ChangeRate, "-0.0037")

	modify := `{"header":{"tr_cd":"SC2"},"body":{"ordxctptncode":"12","ordno":"86383","orgordno":"86382","shtnIsuno":"A005930","bnstp":"2","ordqty":"1","ordprc":"70000","mdfycnfqty":"1","mdfycnfprc":"70000","unercqty":"1","exectime":"100045203"}}`
	events := ParseLsOrderEvents(parseFrame(t, modify), today)
	if len(events) != 1 || events[0].Type != OrderModified || events[0].OriginalOrderID != "86382" || events[0].Price.String() != "70000" {
		t.Fatalf("modified = %+v", events)
	}
	reject := `{"header":{"tr_cd":"SC4"},"body":{"ordno":"90000","orgordno":"0","shtnIsuno":"A005930","bnstp":"2","rjtqty":"1","ordprc":"1","msgcode":"0040","exectime":"100100000"}}`
	events = ParseLsOrderEvents(parseFrame(t, reject), today)
	if len(events) != 1 || events[0].Type != OrderRejected || events[0].Reason != "0040" || events[0].OriginalOrderID != "" {
		t.Fatalf("rejected = %+v", events)
	}
	// ordxctptncode 가 tr_cd 보다 우선한다
	precedence := strings.Replace(modify, `"tr_cd":"SC2"`, `"tr_cd":"SC1"`, 1)
	if events = ParseLsOrderEvents(parseFrame(t, precedence), today); len(events) != 1 || events[0].Type != OrderModified {
		t.Fatalf("precedence = %+v", events)
	}
}

func TestLsStreamFixture(t *testing.T) {
	fx := loadStreamFixture(t, "ls")
	if fx.Measured {
		t.Fatal("ls 픽스처는 문서 기반(measured=false)이어야 한다")
	}
	var ticks []TradeTick
	for _, raw := range fx.Frames {
		if tick, ok := ParseLsTrade(parseFrame(t, raw), fixtureToday); ok {
			ticks = append(ticks, tick)
		}
	}
	assertStreamTicks(t, ticks, fx)

	var books []OrderBookTick
	for _, raw := range fx.OrderBook.Frames {
		if book, ok := ParseLsOrderBook(parseFrame(t, raw), fixtureToday); ok {
			books = append(books, book)
		}
	}
	assertBooks(t, books, fx.OrderBook)

	var events []OrderEvent
	for _, raw := range fx.OrderEvents.Frames {
		events = append(events, ParseLsOrderEvents(parseFrame(t, raw), fixtureToday)...)
	}
	assertOrderEvents(t, events, fx.OrderEvents)
}
