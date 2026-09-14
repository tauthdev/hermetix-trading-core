package hermetix

// 웹소켓 흐름 검증 — httptest 서버를 웹소켓으로 업그레이드해 KIS/키움 프로토콜(구독·로그인·에코·재접속)을 재생한다.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// serverConn - 서버 쪽에서 본 연결 하나: 받은 메시지 큐와 보낼 수 있는 소켓.
type serverConn struct {
	conn     *websocket.Conn
	received chan string
}

func (c *serverConn) send(t *testing.T, text string) {
	t.Helper()
	if err := c.conn.Write(context.Background(), websocket.MessageText, []byte(text)); err != nil {
		t.Fatalf("server send: %v", err)
	}
}

func (c *serverConn) take(t *testing.T) string {
	t.Helper()
	select {
	case msg := <-c.received:
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("5s 안에 메시지가 도착하지 않음")
		return ""
	}
}

func newWsServer(t *testing.T) (*httptest.Server, chan *serverConn) {
	t.Helper()
	connections := make(chan *serverConn, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		sc := &serverConn{conn: conn, received: make(chan string, 64)}
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

func waitConn(t *testing.T, connections chan *serverConn, timeout time.Duration) *serverConn {
	t.Helper()
	select {
	case c := <-connections:
		return c
	case <-time.After(timeout):
		t.Fatal("접속이 오지 않음")
		return nil
	}
}

func wsURL(srv *httptest.Server) string { return strings.Replace(srv.URL, "http://", "ws://", 1) }

func waitTick(t *testing.T, ticks chan TradeTick) TradeTick {
	t.Helper()
	select {
	case tick := <-ticks:
		return tick
	case <-time.After(5 * time.Second):
		t.Fatal("5s 안에 틱이 도착하지 않음")
		return TradeTick{}
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}

// ---------------------------------------------------------------- KIS

func TestKisStreamSubscribeAndTick(t *testing.T) {
	srv, connections := newWsServer(t)
	stream := newKisMarketStream(wsURL(srv), "P", func() (string, error) { return "APPROVAL-KEY", nil })
	defer stream.Close()
	ticks := make(chan TradeTick, 16)
	stream.SubscribeTrades([]string{"KRX:005930"}, func(tick TradeTick) { ticks <- tick })
	stream.Connect()

	conn := waitConn(t, connections, 5*time.Second)
	var subscribe struct {
		Header struct {
			ApprovalKey string `json:"approval_key"`
			TrType      string `json:"tr_type"`
		} `json:"header"`
		Body struct {
			Input struct {
				TrID  string `json:"tr_id"`
				TrKey string `json:"tr_key"`
			} `json:"input"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(conn.take(t)), &subscribe); err != nil {
		t.Fatal(err)
	}
	if subscribe.Header.ApprovalKey != "APPROVAL-KEY" || subscribe.Header.TrType != "1" ||
		subscribe.Body.Input.TrID != "H0STCNT0" || subscribe.Body.Input.TrKey != "005930" {
		t.Fatalf("subscribe = %+v", subscribe)
	}

	conn.send(t, `{"header":{"tr_id":"H0STCNT0","tr_key":"005930","encrypt":"N"},"body":{"rt_cd":"0","msg_cd":"OPSP0000","msg1":"SUBSCRIBE SUCCESS"}}`)
	conn.send(t, "0|H0STCNT0|001|005930^093012^71500^5^300^-0.42^71480^71800^71900^71300^71500^71400^15^1234567^88000000000^1200^1300^100^105.2^600000")
	tick := waitTick(t, ticks)
	if tick.Symbol != "KRX:005930" || tick.Price.String() != "71500" || tick.Quantity.String() != "15" {
		t.Fatalf("tick = %+v", tick)
	}
	if !stream.IsConnected() {
		t.Fatal("connected 여야 한다")
	}
}

func TestKisStreamEchoesPingPong(t *testing.T) {
	srv, connections := newWsServer(t)
	stream := newKisMarketStream(wsURL(srv), "P", func() (string, error) { return "k", nil })
	defer stream.Close()
	stream.Connect()
	conn := waitConn(t, connections, 5*time.Second)
	ping := `{"header":{"tr_id":"PINGPONG","datetime":"20260914093000"}}`
	conn.send(t, ping)
	if got := conn.take(t); got != ping {
		t.Fatalf("echo = %s", got)
	}
}

func TestKisStreamReconnectsAndResubscribes(t *testing.T) {
	srv, connections := newWsServer(t)
	stream := newKisMarketStream(wsURL(srv), "P", func() (string, error) { return "k", nil })
	defer stream.Close()
	stream.SubscribeTrades([]string{"005930"}, func(TradeTick) {})
	stream.Connect()
	first := waitConn(t, connections, 5*time.Second)
	first.take(t) // 첫 구독
	_ = first.conn.Close(websocket.StatusNormalClosure, "server going away")

	second := waitConn(t, connections, 10*time.Second)
	if !strings.Contains(second.take(t), `"tr_key":"005930"`) {
		t.Fatal("재접속 후 구독을 다시 보내야 한다")
	}
}

func TestKisStreamLateSubscribeSentImmediately(t *testing.T) {
	srv, connections := newWsServer(t)
	stream := newKisMarketStream(wsURL(srv), "P", func() (string, error) { return "k", nil })
	defer stream.Close()
	stream.Connect()
	conn := waitConn(t, connections, 5*time.Second)
	waitFor(t, stream.IsConnected, 5*time.Second, "connect")
	stream.SubscribeTrades([]string{"000660"}, func(TradeTick) {})
	if !strings.Contains(conn.take(t), `"tr_key":"000660"`) {
		t.Fatal("연결된 뒤 추가 구독은 즉시 전송")
	}
}

// ---------------------------------------------------------------- Kiwoom

func TestKiwoomStreamLoginRegisterAndTick(t *testing.T) {
	srv, connections := newWsServer(t)
	stream := newKiwoomMarketStream(wsURL(srv)+"/api/dostk/websocket", func() (string, error) { return "ACCESS-TOKEN", nil })
	defer stream.Close()
	ticks := make(chan TradeTick, 16)
	stream.SubscribeTrades([]string{"KRX:005930"}, func(tick TradeTick) { ticks <- tick })
	stream.Connect()

	conn := waitConn(t, connections, 5*time.Second)
	login := conn.take(t)
	if !strings.Contains(login, `"trnm":"LOGIN"`) || !strings.Contains(login, `"token":"ACCESS-TOKEN"`) {
		t.Fatalf("login = %s", login)
	}
	if stream.IsConnected() {
		t.Fatal("로그인 응답 전에는 connected 가 아니다")
	}
	conn.send(t, `{"trnm":"LOGIN","return_code":0,"return_msg":"","sor_yn":"Y"}`)
	reg := conn.take(t)
	if !strings.Contains(reg, `"trnm":"REG"`) || !strings.Contains(reg, `"item":["005930"]`) || !strings.Contains(reg, `"type":["0B"]`) {
		t.Fatalf("reg = %s", reg)
	}
	conn.send(t, `{"trnm":"REG","return_code":0,"return_msg":""}`)
	conn.send(t, `{"data":[{"values":{"20":"093012","10":"-71500","11":"-300","12":"-0.42","27":"+71500","28":"+71400","15":"-15","13":"1234567"},"type":"0B","name":"주식체결","item":"A005930"}],"trnm":"REAL"}`)
	tick := waitTick(t, ticks)
	if tick.Symbol != "KRX:005930" || tick.Price.String() != "71500" || tick.Quantity.String() != "15" {
		t.Fatalf("tick = %+v", tick)
	}
	waitFor(t, stream.IsConnected, 5*time.Second, "로그인 후 connected")
}

func TestKiwoomStreamEchoesPing(t *testing.T) {
	srv, connections := newWsServer(t)
	stream := newKiwoomMarketStream(wsURL(srv), func() (string, error) { return "t", nil })
	defer stream.Close()
	stream.Connect()
	conn := waitConn(t, connections, 5*time.Second)
	conn.take(t) // LOGIN
	ping := `{"trnm":"PING"}`
	conn.send(t, ping)
	if got := conn.take(t); got != ping {
		t.Fatalf("echo = %s", got)
	}
}

func TestKiwoomStreamReconnectsReloginsAndRegisters(t *testing.T) {
	srv, connections := newWsServer(t)
	stream := newKiwoomMarketStream(wsURL(srv), func() (string, error) { return "t", nil })
	defer stream.Close()
	stream.SubscribeTrades([]string{"005930"}, func(TradeTick) {})
	stream.Connect()
	first := waitConn(t, connections, 5*time.Second)
	first.take(t) // LOGIN
	_ = first.conn.Close(websocket.StatusNormalClosure, "bye")

	second := waitConn(t, connections, 10*time.Second)
	if !strings.Contains(second.take(t), `"trnm":"LOGIN"`) {
		t.Fatal("재접속 후 LOGIN")
	}
	second.send(t, `{"trnm":"LOGIN","return_code":0}`)
	if !strings.Contains(second.take(t), `"trnm":"REG"`) {
		t.Fatal("재로그인 후 REG")
	}
}

// ---------------------------------------------------------------- 2차 채널: 호가·주문 통보

const (
	kisTestKey = "zkkljxnqkyodprlmaksyyilhzjmxqjer" // 32자 (실측 구독 응답과 같은 형식)
	kisTestIV  = "d82e2f422913e3b2"                 // 16자
)

func kisBookFields() string {
	fields := []string{"005930", "105530", "0"}
	for i := 0; i < 10; i++ {
		fields = append(fields, strconv.Itoa(250500+500*i))
	}
	for i := 0; i < 10; i++ {
		fields = append(fields, strconv.Itoa(250000-500*i))
	}
	for i := 0; i < 10; i++ {
		fields = append(fields, strconv.Itoa(1000*(i+1)))
	}
	for i := 0; i < 10; i++ {
		fields = append(fields, strconv.Itoa(2000*(i+1)))
	}
	fields = append(fields, "55000", "65000", "0", "0")
	return strings.Join(fields, "^")
}

func TestKisStreamOrderBook(t *testing.T) {
	srv, connections := newWsServer(t)
	stream := newKisMarketStream(wsURL(srv), "P", func() (string, error) { return "k", nil })
	defer stream.Close()
	books := make(chan OrderBookTick, 16)
	if err := stream.SubscribeOrderBook([]string{"KRX:005930"}, func(tick OrderBookTick) { books <- tick }); err != nil {
		t.Fatal(err)
	}
	stream.Connect()
	conn := waitConn(t, connections, 5*time.Second)
	if sub := conn.take(t); !strings.Contains(sub, `"tr_id":"H0STASP0"`) || !strings.Contains(sub, `"tr_key":"005930"`) {
		t.Fatalf("subscribe = %s", sub)
	}
	conn.send(t, "0|H0STASP0|001|"+kisBookFields())
	var book OrderBookTick
	select {
	case book = <-books:
	case <-time.After(5 * time.Second):
		t.Fatal("호가가 오지 않음")
	}
	ask, _ := book.BestAsk()
	bid, _ := book.BestBid()
	if book.Symbol != "KRX:005930" || len(book.Asks) != 10 || ask.Price.String() != "250500" || ask.Quantity.String() != "1000" ||
		bid.Price.String() != "250000" || bid.Quantity.String() != "2000" || book.Asks[9].Price.String() != "255000" {
		t.Fatalf("book = %+v", book)
	}
	assertDecimalEqual(t, "totalAsk", book.TotalAskQuantity, "55000")
	assertDecimalEqual(t, "totalBid", book.TotalBidQuantity, "65000")
}

func TestKisStreamOrderEventsDecrypted(t *testing.T) {
	srv, connections := newWsServer(t)
	stream := newKisMarketStream(wsURL(srv), "P", func() (string, error) { return "k", nil })
	stream.htsID = "HTSUSER"
	defer stream.Close()
	events := make(chan OrderEvent, 16)
	if err := stream.SubscribeOrderEvents(func(e OrderEvent) { events <- e }); err != nil {
		t.Fatal(err)
	}
	stream.Connect()
	conn := waitConn(t, connections, 5*time.Second)
	if sub := conn.take(t); !strings.Contains(sub, `"tr_id":"H0STCNI9"`) || !strings.Contains(sub, `"tr_key":"HTSUSER"`) {
		t.Fatalf("subscribe = %s", sub)
	}
	conn.send(t, `{"header":{"tr_id":"H0STCNI9","tr_key":"HTSUSER","encrypt":"Y"},"body":{"rt_cd":"0","msg_cd":"OPSP0000","msg1":"SUBSCRIBE SUCCESS","output":{"iv":"`+kisTestIV+`","key":"`+kisTestKey+`"}}}`)
	accepted := "HTSUSER^50199202^0000012345^0000000000^02^0^00^0^005930^0^0^105530^0^1^1^00950^3^홍길동^0^^N^^^^삼성전자^250000"
	filled := "HTSUSER^50199202^0000012345^0000000000^02^0^00^0^005930^3^250000^105531^0^2^2^00950^3^홍길동^0^^N^^^^삼성전자^250000"
	for _, plain := range []string{accepted, filled} {
		enc, err := KisEncrypt(plain, kisTestKey, kisTestIV)
		if err != nil {
			t.Fatal(err)
		}
		conn.send(t, "1|H0STCNI9|001|"+enc)
	}
	take := func() OrderEvent {
		select {
		case e := <-events:
			return e
		case <-time.After(5 * time.Second):
			t.Fatal("주문 통보가 오지 않음")
			return OrderEvent{}
		}
	}
	e1 := take()
	if e1.Type != OrderAccepted || e1.OrderID != "0000012345" || !e1.OrderIDMatches("12345") || e1.Symbol != "005930" || e1.Side == nil || *e1.Side != Buy {
		t.Fatalf("e1 = %+v", e1)
	}
	assertDecimalEqual(t, "quantity", e1.Quantity, "3")
	assertDecimalEqual(t, "price", e1.Price, "250000")
	e2 := take()
	if e2.Type != OrderFilled {
		t.Fatalf("e2 = %+v", e2)
	}
	assertDecimalEqual(t, "fill quantity", e2.Quantity, "3")
	assertDecimalEqual(t, "fill price", e2.Price, "250000")
}

func TestKisStreamOrderEventsRequireHTSID(t *testing.T) {
	stream := newKisMarketStream("ws://127.0.0.1:1", "P", func() (string, error) { return "k", nil })
	defer stream.Close()
	if err := stream.SubscribeOrderEvents(func(OrderEvent) {}); err == nil || !strings.Contains(err.Error(), "HTS ID") {
		t.Fatalf("err = %v", err)
	}
}

func TestKisCryptoRoundTrip(t *testing.T) {
	plain := "005930^105530^250000"
	enc, err := KisEncrypt(plain, kisTestKey, kisTestIV)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := KisDecrypt(enc, kisTestKey, kisTestIV)
	if err != nil || dec != plain {
		t.Fatalf("dec = %q err = %v", dec, err)
	}
}

func kiwoomBookFrame() string {
	values := map[string]string{"21": "105530", "121": "55000", "125": "65000"}
	for i := 0; i < 10; i++ {
		values[strconv.Itoa(41+i)] = "-" + strconv.Itoa(250500+500*i)
		values[strconv.Itoa(61+i)] = strconv.Itoa(1000 * (i + 1))
		values[strconv.Itoa(51+i)] = "-" + strconv.Itoa(250000-500*i)
		values[strconv.Itoa(71+i)] = strconv.Itoa(2000 * (i + 1))
	}
	raw, _ := json.Marshal(map[string]any{
		"data": []map[string]any{{"values": values, "type": "0D", "name": "주식호가잔량", "item": "A005930"}},
		"trnm": "REAL",
	})
	return string(raw)
}

const kiwoomOrderFrame = `{"data":[{"values":{"9203":"0000012345","904":"0000000000","9001":"A005930","913":"체결","905":"+매수","907":"2","900":"1","901":"+250000","902":"0","910":"+250000","911":"1","908":"105531","919":""},"type":"00","name":"주문체결","item":""}],"trnm":"REAL"}`

func TestKiwoomStreamOrderBookAndOrderEvents(t *testing.T) {
	srv, connections := newWsServer(t)
	stream := newKiwoomMarketStream(wsURL(srv), func() (string, error) { return "t", nil })
	defer stream.Close()
	books := make(chan OrderBookTick, 16)
	events := make(chan OrderEvent, 16)
	if err := stream.SubscribeOrderBook([]string{"KRX:005930"}, func(tick OrderBookTick) { books <- tick }); err != nil {
		t.Fatal(err)
	}
	if err := stream.SubscribeOrderEvents(func(e OrderEvent) { events <- e }); err != nil {
		t.Fatal(err)
	}
	stream.Connect()
	conn := waitConn(t, connections, 5*time.Second)
	conn.take(t) // LOGIN
	conn.send(t, `{"trnm":"LOGIN","return_code":0}`)
	regs := []string{conn.take(t), conn.take(t)}
	sawBook, sawOrders := false, false
	for _, reg := range regs {
		if strings.Contains(reg, `"type":["0D"]`) && strings.Contains(reg, `"item":["005930"]`) {
			sawBook = true
		}
		if strings.Contains(reg, `"type":["00"]`) && strings.Contains(reg, `"item":[""]`) {
			sawOrders = true
		}
	}
	if !sawBook || !sawOrders {
		t.Fatalf("regs = %v", regs)
	}

	conn.send(t, kiwoomBookFrame())
	var book OrderBookTick
	select {
	case book = <-books:
	case <-time.After(5 * time.Second):
		t.Fatal("호가가 오지 않음")
	}
	ask, _ := book.BestAsk()
	bid, _ := book.BestBid()
	if book.Symbol != "KRX:005930" || ask.Price.String() != "250500" || ask.Quantity.String() != "1000" || bid.Price.String() != "250000" || book.Bids[1].Price.String() != "249500" {
		t.Fatalf("book = %+v", book)
	}
	assertDecimalEqual(t, "totalAsk", book.TotalAskQuantity, "55000")

	conn.send(t, kiwoomOrderFrame)
	var e OrderEvent
	select {
	case e = <-events:
	case <-time.After(5 * time.Second):
		t.Fatal("주문 통보가 오지 않음")
	}
	if e.Type != OrderFilled || e.OrderID != "0000012345" || e.Symbol != "005930" || e.Side == nil || *e.Side != Buy {
		t.Fatalf("event = %+v", e)
	}
	assertDecimalEqual(t, "quantity", e.Quantity, "1")
	assertDecimalEqual(t, "price", e.Price, "250000")
	assertDecimalEqual(t, "remaining", e.RemainingQuantity, "0")
}

func TestKiwoomOrderEventParsing(t *testing.T) {
	frame := func(status, kind, filledQty, reason string) []byte {
		return []byte(`{"trnm":"REAL","data":[{"type":"00","name":"주문체결","item":"A005930","values":{"9203":"0000012346","904":"0000000000","9001":"A005930","913":"` + status + `","905":"` + kind + `","907":"2","900":"1","901":"+240000","902":"1","910":"","911":"` + filledQty + `","908":"105530","919":"` + reason + `"}}]}`)
	}
	accepted := ParseKiwoomOrderEvents(frame("접수", "+매수", "0", ""), fixtureToday)
	if len(accepted) != 1 || accepted[0].Type != OrderAccepted {
		t.Fatalf("accepted = %+v", accepted)
	}
	assertDecimalEqual(t, "quantity", accepted[0].Quantity, "1")
	assertDecimalEqual(t, "price", accepted[0].Price, "240000")
	if e := ParseKiwoomOrderEvents(frame("확인", "매수취소", "0", ""), fixtureToday); e[0].Type != OrderCanceled {
		t.Fatalf("canceled = %+v", e)
	}
	if e := ParseKiwoomOrderEvents(frame("확인", "매수정정", "0", ""), fixtureToday); e[0].Type != OrderModified {
		t.Fatalf("modified = %+v", e)
	}
	if e := ParseKiwoomOrderEvents(frame("거부", "+매수", "0", "주문가능금액 부족"), fixtureToday); e[0].Type != OrderRejected || e[0].Reason != "주문가능금액 부족" {
		t.Fatalf("rejected = %+v", e)
	}
}

func TestKisOrderEventParsingCancelReject(t *testing.T) {
	canceled := "U^A^0000000002^0000000001^01^2^00^0^005930^0^0^105530^0^1^2^00950^3^N^0^^N^^^^S^0"
	rejected := "U^A^0000000003^0000000000^02^0^00^0^005930^0^0^105530^1^1^1^00950^3^N^0^^N^^^^S^0"
	c := ParseKisOrderEvents("0|H0STCNI9|001|"+canceled, fixtureToday)
	if len(c) != 1 || c[0].Type != OrderCanceled || c[0].Side == nil || *c[0].Side != Sell || c[0].OriginalOrderID != "0000000001" {
		t.Fatalf("canceled = %+v", c)
	}
	r := ParseKisOrderEvents("0|H0STCNI0|001|"+rejected, fixtureToday)
	if len(r) != 1 || r[0].Type != OrderRejected {
		t.Fatalf("rejected = %+v", r)
	}
	if other := ParseKisOrderEvents("0|H0STCNT0|001|"+canceled, fixtureToday); len(other) != 0 {
		t.Fatal("다른 TR 은 무시")
	}
}
