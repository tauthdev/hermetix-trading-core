package hermetix

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
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
}

func newThrottle(interval time.Duration) *throttle {
	return &throttle{interval: interval}
}

func (t *throttle) wait() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if delta := t.interval - time.Since(t.last); delta > 0 {
		time.Sleep(delta)
	}
	t.last = time.Now()
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
