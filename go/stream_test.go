package hermetix

// 웹소켓 흐름 검증 — httptest 서버를 웹소켓으로 업그레이드해 KIS/키움 프로토콜(구독·로그인·에코·재접속)을 재생한다.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
