package hermetix

// 키움증권 REST 모의투자 어댑터 (KRX). 실측 기반 (2026-08).
//
//   - POST + api-id 헤더 라우팅 / return_code(0=성공)
//   - 가격에 등락 부호 접두 (cur_prc "-239500") -> 절대값 / 금액은 zero-padded
//   - TR당 초당 1회 유량 제한 -> 1.1s 쓰로틀 + 백오프

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

type KiwoomClient struct {
	appkey, secretkey string
	baseURL           string
	customBaseURL     bool
	environment       TradingEnvironment
	http              *http.Client
	throttle          *throttle
	tokenMu           sync.Mutex
	token             string
	tokenExpires      time.Time
	call              func(path, apiID string, jsonBody map[string]string) (map[string]any, error)
}

const (
	KiwoomPaperURL = "https://mockapi.kiwoom.com"
	KiwoomLiveURL  = "https://api.kiwoom.com"
)

func NewKiwoomClient(appkey, secretkey string) *KiwoomClient {
	c := &KiwoomClient{
		appkey: appkey, secretkey: secretkey,
		baseURL:     KiwoomPaperURL,
		environment: Paper,
		http:        &http.Client{Timeout: 30 * time.Second},
		throttle:    newThrottle(1100 * time.Millisecond),
	}
	c.call = c.request
	return c
}

// SetBaseURL - 호스트를 직접 지정 (환경 자동 결정 무시).
func (c *KiwoomClient) SetBaseURL(baseURL string) *KiwoomClient {
	c.baseURL = baseURL
	c.customBaseURL = true
	return c
}

// SetEnvironment - 거래 환경 지정. 호스트(모의 mockapi / 실전 api.kiwoom.com)가 결정된다. TR ID 는 공통.
func (c *KiwoomClient) SetEnvironment(env TradingEnvironment) *KiwoomClient {
	c.environment = env
	if !c.customBaseURL {
		c.baseURL = KiwoomPaperURL
		if env == Live {
			c.baseURL = KiwoomLiveURL
		}
	}
	return c
}

func (c *KiwoomClient) Environment() TradingEnvironment { return c.environment }

// BaseURL - 현재 적용된 호스트.
func (c *KiwoomClient) BaseURL() string { return c.baseURL }

func (c *KiwoomClient) Capabilities() BrokerCapabilities {
	return BrokerCapabilities{
		BrokerID: "kiwoom", Market: "KRX", Currency: "KRW",
		CandleIntervals:  map[CandleInterval]bool{Day1: true},
		ClientOrderID:    false,
		NativeBracket:    false,
		FractionalShares: false,
		ServerOpenOrders: true, // ka10075 미체결 조회 제공
		Environments:     map[TradingEnvironment]bool{Paper: true, Live: true},
	}
}

// ------------------------------------------------------------------- market

func (c *KiwoomClient) GetQuotes(symbols []string) ([]Quote, error) {
	quotes := make([]Quote, 0, len(symbols))
	for _, symbol := range symbols {
		body, err := c.call("/api/dostk/stkinfo", "ka10001", map[string]string{"stk_cd": SymbolCode(symbol)})
		if err != nil {
			return nil, err
		}
		quote := Quote{
			Symbol: symbol, Price: d(body["cur_prc"]).Abs(),
			Volume: d(body["trde_qty"]).Abs().IntPart(),
			Change: dOrNil(body["pred_pre"]), Timestamp: time.Now(),
		}
		if rate := dOrNil(body["flu_rt"]); rate != nil {
			converted := rate.Div(decimal.NewFromInt(100)) // % -> 비율
			quote.ChangeRate = &converted
		}
		quotes = append(quotes, quote)
	}
	return quotes, nil
}

func (c *KiwoomClient) GetCandles(symbol string, interval CandleInterval, limit int) ([]Candle, error) {
	if interval != Day1 {
		return nil, fmt.Errorf("키움 어댑터는 일봉(1d)만 지원합니다")
	}
	body, err := c.call("/api/dostk/chart", "ka10081", map[string]string{
		"stk_cd": SymbolCode(symbol), "base_dt": time.Now().In(kst).Format("20060102"), "upd_stkpc_tp": "1",
	})
	if err != nil {
		return nil, err
	}
	candles := make([]Candle, 0)
	for _, r := range rows(body, "stk_dt_pole_chart_qry") {
		date := str(r["dt"])
		if date == "" {
			continue
		}
		ts, err := time.ParseInLocation("20060102", date, kst)
		if err != nil {
			continue
		}
		candles = append(candles, Candle{
			Timestamp: ts,
			Open:      d(r["open_pric"]).Abs(), High: d(r["high_pric"]).Abs(),
			Low: d(r["low_pric"]).Abs(), Close: d(r["cur_prc"]).Abs(),
			Volume: d(r["trde_qty"]).Abs().IntPart(),
		})
	}
	// 키움 최신순 -> 과거→최신
	for i, j := 0, len(candles)-1; i < j; i, j = i+1, j-1 {
		candles[i], candles[j] = candles[j], candles[i]
	}
	if limit > 0 && len(candles) > limit {
		candles = candles[len(candles)-limit:]
	}
	return candles, nil
}

func (c *KiwoomClient) GetCalendar() ([]MarketDay, error) {
	return krxCalendar(31), nil
}

// ------------------------------------------------------------------ account

func (c *KiwoomClient) GetAccount() (Account, error) {
	deposit, err := c.call("/api/dostk/acnt", "kt00001", map[string]string{"qry_tp": "3"})
	if err != nil {
		return Account{}, err
	}
	balance, err := c.balance()
	if err != nil {
		return Account{}, err
	}
	cash := d(deposit["entr"])
	portfolio := d(balance["prsm_dpst_aset_amt"])
	if !portfolio.IsPositive() {
		portfolio = cash.Add(d(balance["tot_evlt_amt"]))
	}
	return Account{AccountID: "kiwoom-mock", Currency: "KRW", Cash: cash, PortfolioValue: portfolio, Status: "ACTIVE"}, nil
}

func (c *KiwoomClient) GetHoldings() ([]Holding, error) {
	balance, err := c.balance()
	if err != nil {
		return nil, err
	}
	holdings := make([]Holding, 0)
	for _, row := range rows(balance, "acnt_evlt_remn_indv_tot") {
		qty := d(row["rmnd_qty"])
		if !qty.IsPositive() {
			continue
		}
		holding := Holding{
			Symbol:   strings.TrimPrefix(str(row["stk_cd"]), "A"),
			Quantity: qty, AvgEntryPrice: d(row["pur_pric"]),
			UnrealizedPnl: dOrNil(row["evltv_prft"]),
		}
		if cur := dOrNil(row["cur_prc"]); cur != nil {
			abs := cur.Abs()
			holding.CurrentPrice = &abs
		}
		if mv := dOrNil(row["evlt_amt"]); mv != nil {
			holding.MarketValue = mv
		}
		if rate := dOrNil(row["prft_rt"]); rate != nil {
			converted := rate.Div(decimal.NewFromInt(100))
			holding.UnrealizedPnlRate = &converted
		}
		holdings = append(holdings, holding)
	}
	return holdings, nil
}

func (c *KiwoomClient) GetBuyingPower() (decimal.Decimal, error) {
	deposit, err := c.call("/api/dostk/acnt", "kt00001", map[string]string{"qry_tp": "3"})
	if err != nil {
		return decimal.Zero, err
	}
	if power := d(deposit["ord_alow_amt"]); power.IsPositive() {
		return power, nil
	}
	return d(deposit["entr"]), nil
}

// ------------------------------------------------------------------- orders

func (c *KiwoomClient) CreateOrder(request CreateOrderRequest) (Order, error) {
	apiID := "kt10001"
	if request.Side == Buy {
		apiID = "kt10000"
	}
	isLimit := request.OrderType == Limit
	ordUv, trdeTp := "", "3"
	if isLimit {
		ordUv = KrxTickRound(*request.LimitPrice).String() // 호가단위 보정
		trdeTp = "0"
	}
	body, err := c.call("/api/dostk/ordr", apiID, map[string]string{
		"dmst_stex_tp": "KRX", "stk_cd": SymbolCode(request.Symbol),
		"ord_qty": request.Quantity.String(), "ord_uv": ordUv, "trde_tp": trdeTp, "cond_uv": "",
	})
	if err != nil {
		return Order{}, err
	}
	now := time.Now()
	zero := decimal.Zero
	return Order{
		OrderID: str(body["ord_no"]), Status: Submitted,
		Symbol: SymbolCode(request.Symbol), Side: request.Side, OrderType: request.OrderType,
		Quantity: &request.Quantity, LimitPrice: request.LimitPrice,
		FilledQuantity: &zero, SubmittedAt: &now,
	}, nil
}

func (c *KiwoomClient) GetOrders() ([]Order, error) {
	open, err := c.openRows()
	if err != nil {
		return nil, err
	}
	orders := make([]Order, 0, len(open))
	for _, row := range open {
		orders = append(orders, kiwoomOpenOrder(row))
	}
	return orders, nil
}

func (c *KiwoomClient) GetOrder(orderID string) (Order, error) {
	key := strings.TrimLeft(orderID, "0")
	open, err := c.openRows()
	if err != nil {
		return Order{}, err
	}
	for _, row := range open {
		if strings.TrimLeft(str(row["ord_no"]), "0") == key {
			return kiwoomOpenOrder(row), nil
		}
	}
	fills, err := c.fillRows()
	if err != nil {
		return Order{}, err
	}
	for _, row := range fills {
		if strings.TrimLeft(str(row["ord_no"]), "0") == key {
			filled := d(row["cntr_qty"])
			order := Order{OrderID: orderID, Status: Filled,
				Symbol: strings.TrimPrefix(str(row["stk_cd"]), "A"), FilledQuantity: &filled}
			if price := dOrNil(row["cntr_pric"]); price != nil {
				abs := price.Abs()
				order.AvgFillPrice = &abs
			}
			return order, nil
		}
	}
	return Order{OrderID: orderID, Status: Canceled}, nil
}

func (c *KiwoomClient) CancelOrder(orderID string) (Order, error) {
	key := strings.TrimLeft(orderID, "0")
	open, err := c.openRows()
	if err != nil {
		return Order{}, err
	}
	for _, row := range open {
		if strings.TrimLeft(str(row["ord_no"]), "0") != key {
			continue
		}
		_, err = c.call("/api/dostk/ordr", "kt10003", map[string]string{
			"dmst_stex_tp": "KRX", "orig_ord_no": orderID,
			"stk_cd": strings.TrimPrefix(str(row["stk_cd"]), "A"), "cncl_qty": "0",
		})
		if err != nil {
			return Order{}, err
		}
		now := time.Now()
		return Order{OrderID: orderID, Status: Canceled, CanceledAt: &now}, nil
	}
	return Order{}, newOrderNotFoundError("order-not-found", "키움 미체결 주문을 찾을 수 없습니다: "+orderID)
}

func (c *KiwoomClient) GetFills() ([]Fill, error) {
	fillRows, err := c.fillRows()
	if err != nil {
		return nil, err
	}
	fills := make([]Fill, 0, len(fillRows))
	for _, row := range fillRows {
		side := Sell
		if strings.Contains(str(row["io_tp_nm"]), "매수") {
			side = Buy
		}
		qty := d(row["cntr_qty"])
		fill := Fill{
			FillID: str(row["ord_no"]), OrderID: str(row["ord_no"]),
			Symbol: strings.TrimPrefix(str(row["stk_cd"]), "A"), Side: side, Quantity: &qty,
		}
		if price := dOrNil(row["cntr_pric"]); price != nil {
			abs := price.Abs()
			fill.Price = &abs
		}
		fills = append(fills, fill)
	}
	return fills, nil
}

// ----------------------------------------------------------------- internal

func kiwoomOpenOrder(row map[string]any) Order {
	ordQty := d(row["ord_qty"])
	remaining := ordQty
	if r := dOrNil(row["oso_qty"]); r != nil {
		remaining = *r
	}
	filled := ordQty.Sub(remaining)
	status := Submitted
	if filled.IsPositive() {
		status = PartiallyFilled
	}
	side := Sell
	if strings.Contains(str(row["io_tp_nm"]), "매수") {
		side = Buy
	}
	order := Order{
		OrderID: str(row["ord_no"]), Status: status,
		Symbol: strings.TrimPrefix(str(row["stk_cd"]), "A"), Side: side,
		OrderType: Limit, Quantity: &ordQty, FilledQuantity: &filled,
	}
	if price := dOrNil(row["ord_pric"]); price != nil {
		abs := price.Abs()
		order.LimitPrice = &abs
	}
	return order
}

func (c *KiwoomClient) openRows() ([]map[string]any, error) {
	body, err := c.call("/api/dostk/acnt", "ka10075",
		map[string]string{"all_stk_tp": "0", "trde_tp": "0", "stk_cd": "", "stex_tp": "0"})
	if err != nil {
		return nil, err
	}
	return rows(body, "oso"), nil
}

func (c *KiwoomClient) fillRows() ([]map[string]any, error) {
	body, err := c.call("/api/dostk/acnt", "ka10076",
		map[string]string{"stk_cd": "", "qry_tp": "0", "sell_tp": "0", "ord_no": "", "stex_tp": "0"})
	if err != nil {
		return nil, err
	}
	return rows(body, "cntr"), nil
}

func (c *KiwoomClient) balance() (map[string]any, error) {
	return c.call("/api/dostk/acnt", "kt00018", map[string]string{"qry_tp": "1", "dmst_stex_tp": "KRX"})
}

func (c *KiwoomClient) request(path, apiID string, jsonBody map[string]string) (map[string]any, error) {
	for attempt := 0; ; attempt++ {
		body, err := c.requestOnce(path, apiID, jsonBody)
		var rateLimited *RateLimitError
		if errors.As(err, &rateLimited) && attempt < 3 {
			time.Sleep(time.Duration(attempt+1) * 1100 * time.Millisecond) // TR당 유량 제한 백오프
			continue
		}
		return body, err
	}
}

func (c *KiwoomClient) requestOnce(path, apiID string, jsonBody map[string]string) (map[string]any, error) {
	c.throttle.wait()
	token, err := c.getToken()
	if err != nil {
		return nil, err
	}
	payload, _ := json.Marshal(jsonBody)
	status, body, err := httpJSON(c.http, "POST", c.baseURL+path, map[string]string{
		"Content-Type":  "application/json;charset=UTF-8",
		"authorization": "Bearer " + token,
		"api-id":        apiID,
	}, payload)
	if err != nil {
		return nil, err
	}
	returnCode := int64(-1)
	if code := dOrNil(body["return_code"]); code != nil {
		returnCode = code.IntPart()
	}
	if status < 200 || status >= 300 || returnCode != 0 {
		code := str(body["return_code"])
		msg := strings.TrimSpace(fmt.Sprintf("키움(%s) %s", apiID, str(body["return_msg"])))
		switch {
		case strings.Contains(msg, "요청 개수를 초과"):
			return nil, newRateLimitError(status, code, msg)
		case strings.Contains(msg, "장종료") || strings.Contains(msg, "RC4058"):
			return nil, newMarketClosedError(status, code, msg)
		case status == 401:
			return nil, newAuthError(status, code, msg)
		default:
			return nil, &BrokerAPIError{status, code, msg}
		}
	}
	return body, nil
}

func (c *KiwoomClient) getToken() (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.token != "" && time.Now().Before(c.tokenExpires.Add(-5*time.Minute)) {
		return c.token, nil
	}
	c.throttle.wait()
	payload, _ := json.Marshal(map[string]string{
		"grant_type": "client_credentials", "appkey": c.appkey, "secretkey": c.secretkey,
	})
	status, body, err := httpJSON(c.http, "POST", c.baseURL+"/oauth2/token",
		map[string]string{"Content-Type": "application/json;charset=UTF-8"}, payload)
	if err != nil {
		return "", err
	}
	returnCode := int64(-1)
	if code := dOrNil(body["return_code"]); code != nil {
		returnCode = code.IntPart()
	}
	token := str(body["token"])
	if status != 200 || returnCode != 0 || token == "" {
		return "", newAuthError(status, str(body["return_code"]), "키움 토큰 발급 실패: "+str(body["return_msg"]))
	}
	c.token = token
	// expires_dt: yyyyMMddHHmmss (KST)
	if expires, err := time.ParseInLocation("20060102150405", str(body["expires_dt"]), kst); err == nil {
		c.tokenExpires = expires
	} else {
		c.tokenExpires = time.Now().Add(24 * time.Hour)
	}
	return c.token, nil
}
