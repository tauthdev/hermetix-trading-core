package hermetix

// 브로커 웹소켓 어댑터의 공용 부품 — coder/websocket 위에 재연결·유휴 감시·직렬 전송을 얹는다 (Kotlin ReconnectingWebSocket 대응).
//
//   - Connect 는 즉시 반환하고 전용 고루틴에서 접속한다. 실패하면 1s → 2s → 4s … maxBackoff 로 재시도
//   - 소켓이 닫히거나 오류가 나면 같은 백오프로 재접속한다. Close 뒤에는 재접속하지 않는다
//   - idleTimeout 동안 프레임이 하나도 없으면 죽은 연결로 보고 끊고 재접속한다 (0 이면 끈다 — NH 처럼 조용한 게 정상인 브로커)
//   - heartbeat > 0 이고 프로토콜이 heartbeatProtocol 을 구현하면 그 주기로 OnHeartbeat 를 부른다 (토스처럼 클라이언트가 먼저 PING 을 보내야 하는 브로커)
//   - 프로토콜이 headerProtocol 을 구현하면 핸드셰이크에 그 HTTP 헤더를 싣는다 (토스: Authorization Bearer)
//   - Send 는 직렬화된다. 리스너·훅의 panic 은 로그만 남긴다

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// streamProtocol - 브로커별로 정하는 것: 접속 URI, 연결 직후 보낼 것(로그인·구독), 텍스트 프레임 처리.
type streamProtocol interface {
	// URI - 매 (재)접속마다 호출된다 — 토큰·승인키 갱신은 여기서
	URI() string
	// OnOpen - 소켓이 열린 직후 (스트림 고루틴)
	OnOpen()
	// OnMessage - 완성된 텍스트 프레임 1건 (스트림 고루틴)
	OnMessage(text string)
	// OnDisconnected - 연결이 끊긴 직후 (재접속 예약 전)
	OnDisconnected()
}

// heartbeatProtocol - 선택 훅: heartbeat 주기마다, 소켓이 열려 있을 때만 호출된다 (스트림 고루틴과 별도 고루틴).
type heartbeatProtocol interface {
	OnHeartbeat()
}

// headerProtocol - 선택 훅: 접속 핸드셰이크에 실을 HTTP 헤더 — 매 (재)접속마다 호출된다.
type headerProtocol interface {
	Headers() map[string]string
}

type reconnectingWebSocket struct {
	name        string
	protocol    streamProtocol
	maxBackoff  time.Duration
	idleTimeout time.Duration
	heartbeat   time.Duration

	mu         sync.Mutex
	conn       *websocket.Conn
	closed     bool
	socketOpen bool
	attempt    int
	lastFrame  time.Time
	sendMu     sync.Mutex
	cancel     context.CancelFunc
	wg         sync.WaitGroup

	// RawFrameHook - 원시 텍스트 프레임 관찰용 훅 (프로토콜 실측·픽스처 채집). 파싱 전에 호출되며 panic 은 무시된다
	RawFrameHook func(string)
}

func newReconnectingWebSocket(name string, protocol streamProtocol) *reconnectingWebSocket {
	return &reconnectingWebSocket{
		name: name, protocol: protocol,
		maxBackoff: 30 * time.Second, idleTimeout: 90 * time.Second,
		lastFrame: time.Now(),
	}
}

// IsSocketOpen - 소켓이 열려 있는지 (로그인 필요 브로커는 프로토콜이 별도 상태를 둔다).
func (w *reconnectingWebSocket) IsSocketOpen() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.socketOpen
}

// Connect - 접속 시작. 즉시 반환.
func (w *reconnectingWebSocket) Connect() {
	w.mu.Lock()
	if w.closed || w.cancel != nil {
		w.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.mu.Unlock()
	w.wg.Add(1)
	go w.loop(ctx)
}

// Send - 텍스트 프레임 전송. 연결이 없으면 false.
func (w *reconnectingWebSocket) Send(text string) bool {
	w.mu.Lock()
	conn := w.conn
	w.mu.Unlock()
	if conn == nil {
		return false
	}
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(text)); err != nil {
		log.Printf("WARN hermetix %s stream: 전송 실패 - %v", w.name, err)
		return false
	}
	return true
}

// Close - 재접속을 멈추고 소켓을 닫는다.
func (w *reconnectingWebSocket) Close() error {
	w.mu.Lock()
	w.closed = true
	conn := w.conn
	w.conn = nil
	w.socketOpen = false
	cancel := w.cancel
	w.mu.Unlock()
	if conn != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "bye")
	}
	if cancel != nil {
		cancel()
	}
	w.wg.Wait()
	return nil
}

// ------------------------------------------------------------------ internals

func (w *reconnectingWebSocket) loop(ctx context.Context) {
	defer w.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := w.session(ctx); err != nil && ctx.Err() == nil {
			w.mu.Lock()
			wasOpen := w.socketOpen
			w.conn = nil
			w.socketOpen = false
			w.mu.Unlock()
			if wasOpen {
				log.Printf("WARN hermetix %s stream: disconnected - %v", w.name, err)
				w.safe("onDisconnected", w.protocol.OnDisconnected)
			} else {
				log.Printf("WARN hermetix %s stream: 접속 실패 - %v", w.name, err)
			}
		}
		if ctx.Err() != nil {
			return
		}
		delay := w.nextBackoff()
		log.Printf("INFO hermetix %s stream: reconnect in %s", w.name, delay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// session - 한 번의 접속 수명. 정상 종료(Close)면 nil, 그 외엔 끊긴 이유.
func (w *reconnectingWebSocket) session(ctx context.Context) error {
	uri := w.protocol.URI()
	w.mu.Lock()
	attempt := w.attempt + 1
	w.mu.Unlock()
	log.Printf("INFO hermetix %s stream: connecting %s (attempt %d)", w.name, uri, attempt)
	dialCtx, cancelDial := context.WithTimeout(ctx, 10*time.Second)
	var opts *websocket.DialOptions
	if hp, ok := w.protocol.(headerProtocol); ok {
		headers := http.Header{}
		for k, v := range hp.Headers() {
			headers.Set(k, v)
		}
		opts = &websocket.DialOptions{HTTPHeader: headers}
	}
	conn, _, err := websocket.Dial(dialCtx, uri, opts)
	cancelDial()
	if err != nil {
		return err
	}
	conn.SetReadLimit(4 << 20)
	w.mu.Lock()
	w.conn = conn
	w.socketOpen = true
	w.attempt = 0
	w.lastFrame = time.Now()
	w.mu.Unlock()
	log.Printf("INFO hermetix %s stream: connected", w.name)
	w.safe("onOpen", w.protocol.OnOpen)

	sessionCtx, cancelSession := context.WithCancel(ctx)
	defer cancelSession()
	if w.idleTimeout > 0 {
		go w.watchIdle(sessionCtx, conn)
	}
	if hb, ok := w.protocol.(heartbeatProtocol); ok && w.heartbeat > 0 {
		go w.runHeartbeat(sessionCtx, hb)
	}

	for {
		kind, data, err := conn.Read(sessionCtx)
		if err != nil {
			_ = conn.Close(websocket.StatusNormalClosure, "")
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		w.mu.Lock()
		w.lastFrame = time.Now()
		w.mu.Unlock()
		if kind != websocket.MessageText {
			continue
		}
		w.dispatch(string(data))
	}
}

func (w *reconnectingWebSocket) watchIdle(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(w.idleTimeout / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.mu.Lock()
			idle := time.Since(w.lastFrame)
			w.mu.Unlock()
			if idle > w.idleTimeout {
				log.Printf("WARN hermetix %s stream: %s 동안 프레임 없음 - 재접속", w.name, w.idleTimeout)
				_ = conn.Close(websocket.StatusGoingAway, "idle timeout")
				return
			}
		}
	}
}

func (w *reconnectingWebSocket) runHeartbeat(ctx context.Context, hb heartbeatProtocol) {
	ticker := time.NewTicker(w.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.IsSocketOpen() {
				w.safe("onHeartbeat", hb.OnHeartbeat)
			}
		}
	}
}

func (w *reconnectingWebSocket) nextBackoff() time.Duration {
	w.mu.Lock()
	n := w.attempt
	w.attempt++
	w.mu.Unlock()
	if n > 10 {
		n = 10
	}
	delay := time.Second << uint(n)
	if delay > w.maxBackoff {
		delay = w.maxBackoff
	}
	return delay
}

func (w *reconnectingWebSocket) dispatch(text string) {
	if hook := w.RawFrameHook; hook != nil {
		w.safe("rawFrameHook", func() { hook(text) })
	}
	w.safe("onMessage", func() { w.protocol.OnMessage(text) })
}

// safe - panic 을 로그로 바꾼다 (한 프레임/리스너 오류가 연결을 끊지 않는다).
func (w *reconnectingWebSocket) safe(label string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("ERROR hermetix %s stream: %s panic - %v", w.name, label, r)
		}
	}()
	fn()
}
