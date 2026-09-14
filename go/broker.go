package hermetix

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// BrokerClient - 증권사 추상화. 구현 규약:
//   - 인증(토큰 갱신 포함)은 어댑터 내부에서 처리한다
//   - 실패는 errors.go 의 타입으로 반환한다
//   - 응답의 방언(부호 접두, zero-padding 등)은 어댑터가 정규화한다
type BrokerClient interface {
	Capabilities() BrokerCapabilities
	// Environment - 이 인스턴스가 연결된 거래 환경. 엔진은 Live 면 명시 동의(LiveTradingEnabled)를 요구한다
	Environment() TradingEnvironment
	GetQuotes(symbols []string) ([]Quote, error)
	GetCandles(symbol string, interval CandleInterval, limit int) ([]Candle, error)
	GetCalendar() ([]MarketDay, error)
	GetAccount() (Account, error)
	GetHoldings() ([]Holding, error)
	GetBuyingPower() (decimal.Decimal, error)
	CreateOrder(request CreateOrderRequest) (Order, error)
	GetOrders() ([]Order, error)
	GetOrder(orderID string) (Order, error)
	CancelOrder(orderID string) (Order, error)
	GetFills() ([]Fill, error)
}

// TradeListener - 체결 틱 수신자. 스트림 고루틴에서 호출되므로 오래 걸리는 일은 하지 않는다 (엔진은 루프로 넘긴다).
type TradeListener func(TradeTick)

// MarketStream - 실시간 시장 데이터 스트림. 규약:
//   - Connect 후 연결이 끊기면 스스로 지수 백오프로 재연결하고, 재연결 시 기존 구독을 다시 보낸다
//   - SubscribeTrades 는 연결 전에 불러도 된다 — 연결되는 순간 전송된다
//   - 리스너가 panic 해도 스트림은 로그만 남기고 계속 간다
//   - Close 뒤에는 재연결하지 않는다
type MarketStream interface {
	// IsConnected - 소켓이 열려 있고 (브로커가 요구하면) 로그인까지 끝났는지
	IsConnected() bool
	Connect()
	SubscribeTrades(symbols []string, listener TradeListener)
	Close() error
}

// StreamingBrokerClient - 실시간 스트림을 제공하는 어댑터. Capabilities().Streams 가 비어있지 않은 어댑터만 구현한다.
type StreamingBrokerClient interface {
	BrokerClient
	// OpenStream - 새 스트림 인스턴스. 연결은 호출자가 Connect 로 시작한다
	OpenStream() MarketStream
}

// httpJSON - (status, parsedJSON) 반환. 4xx/5xx 도 본문 파싱.
func httpJSON(client *http.Client, method, rawURL string, headers map[string]string, body []byte) (int, map[string]any, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, rawURL, reader)
	if err != nil {
		return 0, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return res.StatusCode, nil, err
	}
	parsed := map[string]any{}
	_ = json.Unmarshal(raw, &parsed) // 비 JSON 본문은 빈 맵
	return res.StatusCode, parsed, nil
}

// throttle - 호출 간 최소 간격 보장 (모의 서버 레이트리밋 회피).
type throttle struct {
	mu       sync.Mutex
	last     time.Time
	interval time.Duration
	sleep    func(time.Duration)
	now      func() time.Time
}

func newThrottle(interval time.Duration) *throttle {
	return &throttle{interval: interval, sleep: time.Sleep, now: time.Now}
}

func (t *throttle) wait() {
	if t.interval <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.last.IsZero() {
		if delta := t.interval - t.now().Sub(t.last); delta > 0 {
			t.sleep(delta)
		}
	}
	t.last = t.now()
}

// maxRetryAfter - 서버 Retry-After 를 그대로 믿되 상한을 둔다 (틱 하나가 무한히 막히지 않도록).
const maxRetryAfter = 30 * time.Second

// rateLimiter - 어댑터 공용 레이트리밋 부품: 쓰로틀 + RateLimitError 백오프 재시도.
// 재시도가 소진되면 마지막 에러를 그대로 돌려준다 (엔진은 그때서야 틱을 건너뛴다).
type rateLimiter struct {
	throttle   *throttle
	maxRetries int
	backoff    func(attempt int) time.Duration
	sleep      func(time.Duration)
}

func newRateLimiter(interval time.Duration, maxRetries int, backoff func(attempt int) time.Duration) *rateLimiter {
	return &rateLimiter{throttle: newThrottle(interval), maxRetries: maxRetries, backoff: backoff, sleep: time.Sleep}
}

func (r *rateLimiter) execute(label string, fn func() (map[string]any, error)) (map[string]any, error) {
	for attempt := 0; ; {
		r.throttle.wait()
		body, err := fn()
		var rateLimited *RateLimitError
		if err == nil || !errors.As(err, &rateLimited) || attempt >= r.maxRetries {
			return body, err
		}
		attempt++
		wait := r.backoff(attempt)
		if rateLimited.RetryAfterSeconds > 0 {
			wait = time.Duration(rateLimited.RetryAfterSeconds * float64(time.Second))
			if wait > maxRetryAfter {
				wait = maxRetryAfter
			}
		}
		log.Printf("WARN hermetix rate limit(%s) - retry %d/%d after %s", label, attempt, r.maxRetries, wait)
		r.sleep(wait)
	}
}

// httpJSONHeaders - httpJSON + 응답 헤더 (Retry-After 등).
func httpJSONHeaders(client *http.Client, method, rawURL string, headers map[string]string, body []byte) (int, map[string]any, http.Header, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, rawURL, reader)
	if err != nil {
		return 0, nil, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return res.StatusCode, nil, res.Header, err
	}
	parsed := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &parsed)
	}
	return res.StatusCode, parsed, res.Header, nil
}

var kst = mustLoadKST()

func mustLoadKST() *time.Location {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		panic(err)
	}
	return loc
}

// krxCalendar - KRX 정규장 합성 캘린더 (공휴일 미반영 - 주문은 서버가 거부하므로 안전).
func krxCalendar(days int) []MarketDay {
	result := make([]MarketDay, 0, days)
	today := time.Now().In(kst)
	for offset := 0; offset < days; offset++ {
		d := today.AddDate(0, 0, offset)
		weekday := d.Weekday() != time.Saturday && d.Weekday() != time.Sunday
		day := MarketDay{Date: d.Format("2006-01-02"), Open: weekday, Timezone: "Asia/Seoul"}
		if weekday {
			day.Regular = &SessionHours{Start: "09:00", End: "15:30"}
		}
		result = append(result, day)
	}
	return result
}

// KrxTickRound - KRX 호가단위 보정 (2023-01 개정) - 유효 호가로 내림.
func KrxTickRound(price decimal.Decimal) decimal.Decimal {
	var tick int64
	switch {
	case price.LessThan(decimal.NewFromInt(2_000)):
		tick = 1
	case price.LessThan(decimal.NewFromInt(5_000)):
		tick = 5
	case price.LessThan(decimal.NewFromInt(20_000)):
		tick = 10
	case price.LessThan(decimal.NewFromInt(50_000)):
		tick = 50
	case price.LessThan(decimal.NewFromInt(200_000)):
		tick = 100
	case price.LessThan(decimal.NewFromInt(500_000)):
		tick = 500
	default:
		tick = 1_000
	}
	t := decimal.NewFromInt(tick)
	return price.Div(t).Floor().Mul(t)
}

// d - 필수 Decimal 파싱 (빈 값은 0)
func d(value any) decimal.Decimal {
	if parsed := dOrNil(value); parsed != nil {
		return *parsed
	}
	return decimal.Zero
}

// dOrNil - 선택 Decimal 파싱
func dOrNil(value any) *decimal.Decimal {
	if value == nil {
		return nil
	}
	text := strings.TrimSpace(str(value))
	if text == "" {
		return nil
	}
	parsed, err := decimal.NewFromString(strings.TrimPrefix(text, "+"))
	if err != nil {
		return nil
	}
	return &parsed
}

func str(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return decimal.NewFromFloat(v).String()
	case nil:
		return ""
	default:
		raw, _ := json.Marshal(v)
		return strings.Trim(string(raw), `"`)
	}
}

func rows(body map[string]any, key string) []map[string]any {
	list, _ := body[key].([]any)
	result := make([]map[string]any, 0, len(list))
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			result = append(result, m)
		}
	}
	return result
}

func obj(body map[string]any, key string) map[string]any {
	m, _ := body[key].(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func encodeQuery(query map[string]string) string {
	values := url.Values{}
	for k, v := range query {
		values.Set(k, v)
	}
	return values.Encode()
}

// sortCandles - 과거→최신 정렬 (브로커는 대개 최신순으로 준다).
func sortCandles(candles []Candle) {
	sort.Slice(candles, func(i, j int) bool { return candles[i].Timestamp.Before(candles[j].Timestamp) })
}
