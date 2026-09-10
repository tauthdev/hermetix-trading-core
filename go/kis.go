package hermetix

// 한국투자증권(KIS) 모의투자 어댑터 (KRX). 실측 기반 (2026-08).
//
//   - 초당 요청 제한 -> 600ms 쓰로틀 + EGW00201 백오프 재시도
//   - 토큰 발급 1분당 1회 제한 (토큰 24h 캐시)
//   - 모의 서버는 미체결/체결 조회 미제공 -> 메모리 주문 추적, 체결은 보유수량 변화 근사
//   - 취소는 지점번호 없이 ODNO 만으로 동작 / 캔들은 일봉만 / 지정가는 호가단위 보정

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

type kisTracked struct {
	order       Order
	baselineQty decimal.Decimal
	day         string
}

type KisClient struct {
	appkey, appsecret, cano, acntPrdtCd string
	baseURL                             string
	customBaseURL                       bool
	environment                         TradingEnvironment
	http                                *http.Client
	limiter                             *rateLimiter
	tokenMu                             sync.Mutex
	token                               string
	tokenExpires                        time.Time
	trackedMu                           sync.Mutex
	tracked                             map[string]kisTracked
	call                                func(method, path, trID string, query, jsonBody map[string]string) (map[string]any, error)
}

const (
	KisPaperURL = "https://openapivts.koreainvestment.com:29443"
	KisLiveURL  = "https://openapi.koreainvestment.com:9443"
)

func NewKisClient(appkey, appsecret, cano string) *KisClient {
	c := &KisClient{
		appkey: appkey, appsecret: appsecret, cano: cano, acntPrdtCd: "01",
		baseURL:     KisPaperURL,
		environment: Paper,
		http:        &http.Client{Timeout: 30 * time.Second},
		limiter:     newRateLimiter(600*time.Millisecond, 3, func(attempt int) time.Duration { return time.Duration(attempt) * time.Second }),
		tracked:     map[string]kisTracked{},
	}
	c.call = c.request
	return c
}

// SetBaseURL - 호스트를 직접 지정 (환경 자동 결정 무시).
func (c *KisClient) SetBaseURL(baseURL string) *KisClient {
	c.baseURL = baseURL
	c.customBaseURL = true
	return c
}

// SetEnvironment - 거래 환경 지정. 호스트(모의 openapivts:29443 / 실전 openapi:9443), 쓰로틀(모의 600ms / 실전 100ms),
// 계좌 TR ID 프리픽스(모의 V / 실전 T)가 이에 따라 결정된다.
func (c *KisClient) SetEnvironment(env TradingEnvironment) *KisClient {
	c.environment = env
	if !c.customBaseURL {
		c.baseURL = KisPaperURL
		if env == Live {
			c.baseURL = KisLiveURL
		}
	}
	interval := 600 * time.Millisecond
	if env == Live {
		interval = 100 * time.Millisecond
	}
	c.limiter = newRateLimiter(interval, 3, func(attempt int) time.Duration { return time.Duration(attempt) * time.Second })
	return c
}

// SetThrottle - 호출 간 최소 간격 직접 지정 (테스트용).
func (c *KisClient) SetThrottle(interval time.Duration) *KisClient {
	c.limiter = newRateLimiter(interval, 3, func(attempt int) time.Duration { return time.Duration(attempt) * time.Second })
	return c
}

func (c *KisClient) Environment() TradingEnvironment { return c.environment }

// BaseURL - 현재 적용된 호스트.
func (c *KisClient) BaseURL() string { return c.baseURL }

// tr - 계좌 TR ID: 모의 V, 실전 T 프리픽스 (예: tr("TTC0802U") → VTTC0802U / TTTC0802U).
func (c *KisClient) tr(suffix string) string {
	if c.environment == Live {
		return "T" + suffix
	}
	return "V" + suffix
}

func (c *KisClient) Capabilities() BrokerCapabilities {
	return BrokerCapabilities{
		BrokerID: "kis", Market: "KRX", Currency: "KRW",
		CandleIntervals:  map[CandleInterval]bool{Day1: true},
		ClientOrderID:    false,
		NativeBracket:    false,
		FractionalShares: false,
		ServerOpenOrders: false, // 모의 서버가 주문 조회 미제공 - 어댑터 내부 추적
		Environments:     map[TradingEnvironment]bool{Paper: true, Live: true},
	}
}

// ------------------------------------------------------------------- market

func (c *KisClient) GetQuotes(symbols []string) ([]Quote, error) {
	quotes := make([]Quote, 0, len(symbols))
	for _, symbol := range symbols {
		body, err := c.call("GET", "/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
			map[string]string{"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": SymbolCode(symbol)}, nil)
		if err != nil {
			return nil, err
		}
		out := obj(body, "output")
		quote := Quote{
			Symbol: symbol, Price: d(out["stck_prpr"]),
			Volume: d(out["acml_vol"]).IntPart(),
			Change: dOrNil(out["prdy_vrss"]), Timestamp: time.Now(),
		}
		if rate := dOrNil(out["prdy_ctrt"]); rate != nil {
			converted := rate.Div(decimal.NewFromInt(100)) // % -> 비율
			quote.ChangeRate = &converted
		}
		quotes = append(quotes, quote)
	}
	return quotes, nil
}

func (c *KisClient) GetCandles(symbol string, interval CandleInterval, limit int) ([]Candle, error) {
	if interval != Day1 {
		return nil, fmt.Errorf("KIS 어댑터는 일봉(1d)만 지원합니다")
	}
	if limit <= 0 {
		limit = 30
	}
	today := time.Now().In(kst)
	start := today.AddDate(0, 0, -(limit*16/10 + 10)) // 휴장일 감안 여유 조회
	body, err := c.call("GET", "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
		map[string]string{
			"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": SymbolCode(symbol),
			"FID_INPUT_DATE_1": start.Format("20060102"), "FID_INPUT_DATE_2": today.Format("20060102"),
			"FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0",
		}, nil)
	if err != nil {
		return nil, err
	}
	candles := make([]Candle, 0)
	for _, r := range rows(body, "output2") {
		date := str(r["stck_bsop_date"])
		if date == "" {
			continue
		}
		ts, err := time.ParseInLocation("20060102", date, kst)
		if err != nil {
			continue
		}
		candles = append(candles, Candle{
			Timestamp: ts,
			Open:      d(r["stck_oprc"]), High: d(r["stck_hgpr"]),
			Low: d(r["stck_lwpr"]), Close: d(r["stck_clpr"]),
			Volume: d(r["acml_vol"]).IntPart(),
		})
	}
	// KIS 최신순 -> 과거→최신
	for i, j := 0, len(candles)-1; i < j; i, j = i+1, j-1 {
		candles[i], candles[j] = candles[j], candles[i]
	}
	if len(candles) > limit {
		candles = candles[len(candles)-limit:]
	}
	return candles, nil
}

func (c *KisClient) GetCalendar() ([]MarketDay, error) {
	return krxCalendar(31), nil
}

// ------------------------------------------------------------------ account

func (c *KisClient) GetAccount() (Account, error) {
	body, err := c.balance()
	if err != nil {
		return Account{}, err
	}
	summaries := rows(body, "output2")
	if len(summaries) == 0 {
		return Account{}, &BrokerAPIError{200, "", "KIS 잔고 요약(output2)이 비어 있습니다"}
	}
	return Account{
		AccountID: c.cano, Currency: "KRW",
		Cash: d(summaries[0]["dnca_tot_amt"]), PortfolioValue: d(summaries[0]["tot_evlu_amt"]),
		Status: "ACTIVE",
	}, nil
}

func (c *KisClient) GetHoldings() ([]Holding, error) {
	body, err := c.balance()
	if err != nil {
		return nil, err
	}
	holdings := make([]Holding, 0)
	for _, row := range rows(body, "output1") {
		qty := d(row["hldg_qty"])
		if !qty.IsPositive() {
			continue
		}
		holding := Holding{
			Symbol: str(row["pdno"]), Quantity: qty, AvgEntryPrice: d(row["pchs_avg_pric"]),
			CurrentPrice: dOrNil(row["prpr"]), MarketValue: dOrNil(row["evlu_amt"]),
			UnrealizedPnl: dOrNil(row["evlu_pfls_amt"]),
		}
		if rate := dOrNil(row["evlu_pfls_rt"]); rate != nil {
			converted := rate.Div(decimal.NewFromInt(100))
			holding.UnrealizedPnlRate = &converted
		}
		holdings = append(holdings, holding)
	}
	return holdings, nil
}

func (c *KisClient) GetBuyingPower() (decimal.Decimal, error) {
	query := c.acct()
	query["PDNO"] = "005930"
	query["ORD_UNPR"] = ""
	query["ORD_DVSN"] = "01"
	query["CMA_EVLU_AMT_ICLD_YN"] = "N"
	query["OVRS_ICLD_YN"] = "N"
	body, err := c.call("GET", "/uapi/domestic-stock/v1/trading/inquire-psbl-order", c.tr("TTC8908R"), query, nil)
	if err != nil {
		return decimal.Zero, err
	}
	return d(obj(body, "output")["ord_psbl_cash"]), nil
}

// ------------------------------------------------------------------- orders

func (c *KisClient) CreateOrder(request CreateOrderRequest) (Order, error) {
	trID := c.tr("TTC0801U")
	if request.Side == Buy {
		trID = c.tr("TTC0802U")
	}
	isLimit := request.OrderType == Limit
	price := "0"
	if isLimit {
		price = KrxTickRound(*request.LimitPrice).String() // 호가단위 보정
	}
	ordDvsn := "01"
	if isLimit {
		ordDvsn = "00"
	}
	payload := c.acct()
	code, err := c.Capabilities().SymbolCode(request.Symbol)
	if err != nil {
		return Order{}, err
	}
	payload["PDNO"] = code
	payload["ORD_DVSN"] = ordDvsn
	payload["ORD_QTY"] = request.Quantity.String()
	payload["ORD_UNPR"] = price

	baseline, err := c.holdingQty(request.Symbol)
	if err != nil {
		return Order{}, err
	}
	body, err := c.call("POST", "/uapi/domestic-stock/v1/trading/order-cash", trID, nil, payload)
	if err != nil {
		return Order{}, err
	}
	now := time.Now()
	zero := decimal.Zero
	order := Order{
		OrderID: str(obj(body, "output")["ODNO"]), Status: Submitted,
		Symbol: code, Side: request.Side, OrderType: request.OrderType, // 보유/추적과 같은 단일 시장 표기
		Quantity: &request.Quantity, LimitPrice: request.LimitPrice,
		FilledQuantity: &zero, SubmittedAt: &now,
	}
	c.trackedMu.Lock()
	c.tracked[order.OrderID] = kisTracked{order: order, baselineQty: baseline, day: time.Now().In(kst).Format("2006-01-02")}
	c.trackedMu.Unlock()
	return order, nil
}

func (c *KisClient) GetOrders() ([]Order, error) {
	if err := c.refreshTracked(); err != nil {
		return nil, err
	}
	c.trackedMu.Lock()
	defer c.trackedMu.Unlock()
	orders := make([]Order, 0)
	for _, t := range c.tracked {
		if t.order.Status.IsOpen() {
			orders = append(orders, t.order)
		}
	}
	return orders, nil
}

func (c *KisClient) GetOrder(orderID string) (Order, error) {
	if err := c.refreshTracked(); err != nil {
		return Order{}, err
	}
	c.trackedMu.Lock()
	defer c.trackedMu.Unlock()
	if t, ok := c.tracked[orderID]; ok {
		return t.order, nil
	}
	// 추적 밖(재시작 등)은 알 수 없어 취소로 간주
	return Order{OrderID: orderID, Status: Canceled}, nil
}

func (c *KisClient) CancelOrder(orderID string) (Order, error) {
	// 실측: 모의 서버는 지점번호 없이 ODNO 만으로 취소된다
	payload := c.acct()
	payload["KRX_FWDG_ORD_ORGNO"] = ""
	payload["ORGN_ODNO"] = orderID
	payload["ORD_DVSN"] = "00"
	payload["RVSE_CNCL_DVSN_CD"] = "02"
	payload["ORD_QTY"] = "0"
	payload["ORD_UNPR"] = "0"
	payload["QTY_ALL_ORD_YN"] = "Y"
	if _, err := c.call("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl", c.tr("TTC0803U"), nil, payload); err != nil {
		return Order{}, err
	}
	now := time.Now()
	c.trackedMu.Lock()
	if t, ok := c.tracked[orderID]; ok {
		t.order.Status = Canceled
		t.order.CanceledAt = &now
		c.tracked[orderID] = t
	}
	c.trackedMu.Unlock()
	return Order{OrderID: orderID, Status: Canceled, CanceledAt: &now}, nil
}

func (c *KisClient) GetFills() ([]Fill, error) {
	if err := c.refreshTracked(); err != nil {
		return nil, err
	}
	c.trackedMu.Lock()
	defer c.trackedMu.Unlock()
	fills := make([]Fill, 0)
	for _, t := range c.tracked {
		if t.order.Status == Filled {
			fills = append(fills, Fill{
				FillID: t.order.OrderID, OrderID: t.order.OrderID, Symbol: t.order.Symbol,
				Side: t.order.Side, Quantity: t.order.Quantity, Price: t.order.LimitPrice,
			})
		}
	}
	return fills, nil
}

// ----------------------------------------------------------------- internal

// refreshTracked - 추적 중인 미체결의 체결 여부를 보유수량 변화로 판정 (모의 서버 제약의 근사).
func (c *KisClient) refreshTracked() error {
	today := time.Now().In(kst).Format("2006-01-02")
	c.trackedMu.Lock()
	hasOpen := false
	for id, t := range c.tracked {
		if t.day != today {
			delete(c.tracked, id) // DAY 주문 - 날짜가 바뀌면 소멸
			continue
		}
		if t.order.Status.IsOpen() {
			hasOpen = true
		}
	}
	c.trackedMu.Unlock()
	if !hasOpen {
		return nil
	}

	holdings, err := c.GetHoldings()
	if err != nil {
		return err
	}
	bySymbol := map[string]decimal.Decimal{}
	for _, h := range holdings {
		bySymbol[h.Symbol] = h.Quantity
	}

	c.trackedMu.Lock()
	defer c.trackedMu.Unlock()
	for id, t := range c.tracked {
		if !t.order.Status.IsOpen() {
			continue
		}
		current := bySymbol[t.order.Symbol]
		qty := decimal.Zero
		if t.order.Quantity != nil {
			qty = *t.order.Quantity
		}
		filled := false
		if t.order.Side == Buy {
			filled = current.GreaterThanOrEqual(t.baselineQty.Add(qty))
		} else {
			filled = current.LessThanOrEqual(t.baselineQty.Sub(qty))
		}
		if filled {
			t.order.Status = Filled
			t.order.FilledQuantity = &qty
			c.tracked[id] = t
		}
	}
	return nil
}

func (c *KisClient) holdingQty(symbol string) (decimal.Decimal, error) {
	holdings, err := c.GetHoldings()
	if err != nil {
		return decimal.Zero, err
	}
	for _, h := range holdings {
		if h.Symbol == symbol {
			return h.Quantity, nil
		}
	}
	return decimal.Zero, nil
}

func (c *KisClient) balance() (map[string]any, error) {
	query := c.acct()
	for k, v := range map[string]string{
		"AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02", "UNPR_DVSN": "01",
		"FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00",
		"CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
	} {
		query[k] = v
	}
	return c.call("GET", "/uapi/domestic-stock/v1/trading/inquire-balance", c.tr("TTC8434R"), query, nil)
}

func (c *KisClient) acct() map[string]string {
	return map[string]string{"CANO": c.cano, "ACNT_PRDT_CD": c.acntPrdtCd}
}

func (c *KisClient) request(method, path, trID string, query, jsonBody map[string]string) (map[string]any, error) {
	return c.limiter.execute("KIS "+trID, func() (map[string]any, error) {
		return c.requestOnce(method, path, trID, query, jsonBody)
	})
}

func (c *KisClient) requestOnce(method, path, trID string, query, jsonBody map[string]string) (map[string]any, error) {
	token, err := c.getToken()
	if err != nil {
		return nil, err
	}
	rawURL := c.baseURL + path
	if query != nil {
		rawURL += "?" + encodeQuery(query)
	}
	headers := map[string]string{
		"Content-Type":  "application/json; charset=utf-8",
		"authorization": "Bearer " + token,
		"appkey":        c.appkey, "appsecret": c.appsecret,
		"tr_id": trID, "custtype": "P",
	}
	var payload []byte
	if jsonBody != nil {
		payload, _ = json.Marshal(jsonBody)
	}
	status, body, err := httpJSON(c.http, method, rawURL, headers, payload)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 || str(body["rt_cd"]) != "0" {
		code := str(body["msg_cd"])
		msg := strings.TrimSpace(fmt.Sprintf("KIS(%s) %s", trID, str(body["msg1"])))
		switch {
		case code == "EGW00201":
			return nil, newRateLimitError(status, code, msg)
		case strings.Contains(msg, "장종료") || strings.Contains(msg, "장운영일이 아닙"):
			return nil, newMarketClosedError(status, code, msg)
		case status == 401:
			return nil, newAuthError(status, code, msg)
		default:
			return nil, &BrokerAPIError{status, code, msg}
		}
	}
	return body, nil
}

func (c *KisClient) getToken() (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.token != "" && time.Now().Before(c.tokenExpires.Add(-5*time.Minute)) {
		return c.token, nil
	}
	c.limiter.throttle.wait()
	payload, _ := json.Marshal(map[string]string{
		"grant_type": "client_credentials", "appkey": c.appkey, "appsecret": c.appsecret,
	})
	status, body, err := httpJSON(c.http, "POST", c.baseURL+"/oauth2/tokenP",
		map[string]string{"Content-Type": "application/json"}, payload)
	if err != nil {
		return "", err
	}
	token := str(body["access_token"])
	if status != 200 || token == "" {
		return "", newAuthError(status, str(body["error_code"]),
			"KIS 토큰 발급 실패: "+str(body["error_description"])+" (발급은 1분당 1회 제한)")
	}
	c.token = token
	c.tokenExpires = time.Now().Add(time.Duration(d(body["expires_in"]).IntPart()) * time.Second)
	return c.token, nil
}
