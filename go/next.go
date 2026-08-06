package hermetix

// 넥스트증권 모의투자 어댑터 (미국주식). 실측 기반 (2026-08).
// OAuth client_credentials, 토큰 24h, 401 시 1회 재발급-재시도.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

type NextClient struct {
	clientID     string
	clientSecret string
	accountID    string
	baseURL      string
	http         *http.Client
	tokenMu      sync.Mutex
	token        string
	tokenExpires time.Time
	// 테스트에서 교체 가능한 호출 지점
	call func(method, path string, account bool, jsonBody map[string]any) (map[string]any, error)
}

func NewNextClient(clientID, clientSecret string) *NextClient {
	c := &NextClient{
		clientID: clientID, clientSecret: clientSecret,
		accountID: "acc_main",
		baseURL:   "https://openapi.nextsecurities.dev",
		http:      &http.Client{Timeout: 30 * time.Second},
	}
	c.call = c.request
	return c
}

func (c *NextClient) Capabilities() BrokerCapabilities {
	return BrokerCapabilities{
		BrokerID: "next", Market: "US", Currency: "USD",
		CandleIntervals:  map[CandleInterval]bool{Min1: true, Min5: true, Hour1: true, Day1: true},
		ClientOrderID:    true,
		NativeBracket:    false,
		FractionalShares: false,
		ServerOpenOrders: true,
	}
}

// ------------------------------------------------------------------- market

func (c *NextClient) GetQuotes(symbols []string) ([]Quote, error) {
	body, err := c.call("GET", "/v1/market/quotes?symbols="+strings.Join(symbols, ","), false, nil)
	if err != nil {
		return nil, err
	}
	quotes := make([]Quote, 0)
	for _, q := range rows(body, "quotes") {
		ts, _ := time.Parse(time.RFC3339, str(q["timestamp"]))
		quotes = append(quotes, Quote{
			Symbol: str(q["symbol"]), Price: d(q["price"]),
			BidPrice: dOrNil(q["bidPrice"]), AskPrice: dOrNil(q["askPrice"]),
			Volume: d(q["volume"]).IntPart(),
			Change: dOrNil(q["change"]), ChangeRate: dOrNil(q["changeRate"]),
			Timestamp: ts,
		})
	}
	return quotes, nil
}

func (c *NextClient) GetCandles(symbol string, interval CandleInterval, limit int) ([]Candle, error) {
	path := fmt.Sprintf("/v1/market/candles?symbol=%s&interval=%s", symbol, interval)
	if limit > 0 {
		path += fmt.Sprintf("&limit=%d", limit)
	}
	body, err := c.call("GET", path, false, nil)
	if err != nil {
		return nil, err
	}
	candles := make([]Candle, 0)
	for _, r := range rows(body, "candles") {
		ts, _ := time.Parse(time.RFC3339, str(r["timestamp"]))
		candles = append(candles, Candle{
			Timestamp: ts,
			Open:      d(r["open"]), High: d(r["high"]), Low: d(r["low"]), Close: d(r["close"]),
			Volume: d(r["volume"]).IntPart(),
		})
	}
	return candles, nil
}

func (c *NextClient) GetCalendar() ([]MarketDay, error) {
	body, err := c.call("GET", "/v1/market/calendar", false, nil)
	if err != nil {
		return nil, err
	}
	days := make([]MarketDay, 0)
	for _, r := range rows(body, "calendar") {
		day := MarketDay{
			Date: str(r["date"]), Open: r["open"] == true,
			Timezone: str(r["timezone"]), Holiday: str(r["holiday"]),
		}
		if sessions, ok := r["sessions"].(map[string]any); ok {
			if regular, ok := sessions["regular"].(map[string]any); ok {
				day.Regular = &SessionHours{Start: str(regular["start"]), End: str(regular["end"])}
			}
		}
		if day.Timezone == "" {
			day.Timezone = "America/New_York"
		}
		days = append(days, day)
	}
	return days, nil
}

// ------------------------------------------------------------------ account

func (c *NextClient) GetAccount() (Account, error) {
	body, err := c.call("GET", "/v1/account", true, nil)
	if err != nil {
		return Account{}, err
	}
	return Account{
		AccountID: str(body["accountId"]), Currency: str(body["currency"]),
		Cash: d(body["cash"]), PortfolioValue: d(body["portfolioValue"]),
		Status: str(body["status"]),
	}, nil
}

func (c *NextClient) GetHoldings() ([]Holding, error) {
	body, err := c.call("GET", "/v1/account/holdings", true, nil)
	if err != nil {
		return nil, err
	}
	holdings := make([]Holding, 0)
	for _, h := range rows(body, "holdings") {
		holdings = append(holdings, Holding{
			Symbol: str(h["symbol"]), Quantity: d(h["quantity"]), AvgEntryPrice: d(h["avgEntryPrice"]),
			CurrentPrice: dOrNil(h["currentPrice"]), MarketValue: dOrNil(h["marketValue"]),
			UnrealizedPnl: dOrNil(h["unrealizedPnl"]), UnrealizedPnlRate: dOrNil(h["unrealizedPnlRate"]),
		})
	}
	return holdings, nil
}

func (c *NextClient) GetBuyingPower() (decimal.Decimal, error) {
	body, err := c.call("GET", "/v1/account/buying-power", true, nil)
	if err != nil {
		return decimal.Zero, err
	}
	return d(body["buyingPower"]), nil
}

// ------------------------------------------------------------------- orders

func (c *NextClient) CreateOrder(request CreateOrderRequest) (Order, error) {
	tif := request.TimeInForce
	if tif == "" {
		tif = Day
	}
	payload := map[string]any{
		"symbol": request.Symbol, "side": string(request.Side),
		"orderType": string(request.OrderType), "quantity": request.Quantity.String(),
		"timeInForce": string(tif),
	}
	if request.LimitPrice != nil {
		payload["limitPrice"] = request.LimitPrice.String()
	}
	if request.ClientOrderID != "" {
		payload["clientOrderId"] = request.ClientOrderID
	}
	body, err := c.call("POST", "/v1/orders", true, payload)
	if err != nil {
		return Order{}, err
	}
	return nextOrder(body), nil
}

func (c *NextClient) GetOrders() ([]Order, error) {
	body, err := c.call("GET", "/v1/orders", true, nil)
	if err != nil {
		return nil, err
	}
	orders := make([]Order, 0)
	for _, o := range rows(body, "orders") {
		orders = append(orders, nextOrder(o))
	}
	return orders, nil
}

func (c *NextClient) GetOrder(orderID string) (Order, error) {
	body, err := c.call("GET", "/v1/orders/"+orderID, true, nil)
	if err != nil {
		return Order{}, err
	}
	return nextOrder(body), nil
}

func (c *NextClient) CancelOrder(orderID string) (Order, error) {
	body, err := c.call("DELETE", "/v1/orders/"+orderID, true, nil)
	if err != nil {
		return Order{}, err
	}
	return nextOrder(body), nil
}

func (c *NextClient) GetFills() ([]Fill, error) {
	body, err := c.call("GET", "/v1/orders/fills", true, nil)
	if err != nil {
		return nil, err
	}
	fills := make([]Fill, 0)
	for _, f := range rows(body, "fills") {
		fills = append(fills, Fill{
			FillID: str(f["fillId"]), OrderID: str(f["orderId"]), Symbol: str(f["symbol"]),
			Side: OrderSide(str(f["side"])), Quantity: dOrNil(f["quantity"]), Price: dOrNil(f["price"]),
		})
	}
	return fills, nil
}

// ----------------------------------------------------------------- internal

func nextOrder(body map[string]any) Order {
	status := OrderStatus(str(body["status"]))
	switch status {
	case Submitted, PartiallyFilled, Filled, Canceled, Rejected, Expired:
	default:
		status = Unknown
	}
	order := Order{
		OrderID: str(body["orderId"]), Status: status,
		Symbol: str(body["symbol"]), Side: OrderSide(str(body["side"])),
		OrderType: OrderType(str(body["orderType"])),
		Quantity:  dOrNil(body["quantity"]), LimitPrice: dOrNil(body["limitPrice"]),
		FilledQuantity: dOrNil(body["filledQuantity"]), AvgFillPrice: dOrNil(body["avgFillPrice"]),
		ClientOrderID: str(body["clientOrderId"]),
	}
	if ts, err := time.Parse(time.RFC3339, str(body["submittedAt"])); err == nil {
		order.SubmittedAt = &ts
	}
	if ts, err := time.Parse(time.RFC3339, str(body["canceledAt"])); err == nil {
		order.CanceledAt = &ts
	}
	return order
}

func (c *NextClient) request(method, path string, account bool, jsonBody map[string]any) (map[string]any, error) {
	do := func() (map[string]any, error) {
		token, err := c.getToken()
		if err != nil {
			return nil, err
		}
		headers := map[string]string{"Authorization": "Bearer " + token}
		if account {
			headers["X-Nextsecurities-Account"] = c.accountID
		}
		var payload []byte
		if jsonBody != nil {
			headers["Content-Type"] = "application/json"
			payload, _ = json.Marshal(jsonBody)
		}
		status, body, err := httpJSON(c.http, method, c.baseURL+path, headers, payload)
		if err != nil {
			return nil, err
		}
		if status < 200 || status >= 300 {
			return nil, mapNextError(status, obj(body, "error"))
		}
		return body, nil
	}

	body, err := do()
	var authErr *AuthError
	if errors.As(err, &authErr) {
		c.tokenMu.Lock()
		c.token = "" // 토큰 만료 - 1회 재발급 후 재시도
		c.tokenMu.Unlock()
		return do()
	}
	return body, err
}

func mapNextError(status int, e map[string]any) error {
	code := str(e["code"])
	msg := fmt.Sprintf("Next(%s) %s requestId=%s", code, str(e["message"]), str(e["requestId"]))
	switch {
	case status == 401 || str(e["type"]) == "authentication":
		return newAuthError(status, code, msg)
	case status == 429:
		return newRateLimitError(status, code, msg)
	case code == "order-not-found":
		return newOrderNotFoundError(code, msg)
	case strings.Contains(code, "insufficient"):
		return &InsufficientFundsError{BrokerAPIError{status, code, msg}}
	case code == "trading-halted" || strings.Contains(code, "market-closed"):
		return newMarketClosedError(status, code, msg)
	case str(e["type"]) == "validation":
		return &InvalidOrderError{BrokerAPIError{status, code, msg}}
	default:
		return &BrokerAPIError{status, code, msg}
	}
}

func (c *NextClient) getToken() (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.token != "" && time.Now().Before(c.tokenExpires.Add(-time.Minute)) {
		return c.token, nil
	}
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
	}
	status, body, err := httpJSON(c.http, "POST", c.baseURL+"/v1/oauth/token",
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
		[]byte(form.Encode()))
	if err != nil {
		return "", err
	}
	token := str(body["access_token"])
	if status != 200 || token == "" {
		e := obj(body, "error")
		return "", newAuthError(status, str(e["code"]), "Next 토큰 발급 실패: "+str(e["message"]))
	}
	c.token = token
	c.tokenExpires = time.Now().Add(time.Duration(d(body["expires_in"]).IntPart()) * time.Second)
	return c.token, nil
}
