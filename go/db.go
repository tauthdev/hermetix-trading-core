package hermetix

// DB증권 REST OpenAPI 어댑터.
//
// ⚠️ 문서 기반 구현 (실측 전) — 공식 SDK(DBsecurities/dbsec-open-api) 소스·예제 docstring 과 포털 문서에서
// 엔드포인트·필드명·에러 코드를 역추적했다. 모의서버 실측 전까지 상태는 "미검증".
//   - 모든 API 는 POST, 본문 {"In": {...}}, 응답 rsp_cd/rsp_msg + Out(객체 또는 배열) / Out1. TR 코드는 문서용, 경로로 식별
//   - 헤더 authorization: Bearer + cont_yn/cont_key (+ 법인만 mac_address). appkey 헤더 없음
//   - 토큰 POST /oauth2/token 은 form-urlencoded(appsecretkey), 24h, 발급 1분 1건
//   - 운영/모의 같은 호스트 — 모의 키로만 분기. HTTP 200 + rsp_cd != 00000 이 업무 오류(이때 Out 없음)
//   - 앱 20 TPS 이지만 잔고·체결 2 TPS, 예수금 1 TPS → 500ms 쓰로틀 + IGW00201 지수 백오프
//
// 미확인(실측 필요): 응답 숫자의 JSON 타입, IsuNo 의 A 접두 여부, 일봉 정렬(최신일 우선 추정), PrdyVrss 부호 여부

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const DbBaseURL = "https://openapi.dbsec.co.kr:8443"

var (
	dbAuthCodes          = map[string]bool{"IGW00121": true, "IGW00122": true, "IGW00123": true, "IGW40342": true}
	dbMarketClosedCodes  = map[string]bool{"2611": true, "3589": true, "3590": true, "3563": true}
	dbInsufficientCodes  = map[string]bool{"1584": true, "2714": true, "2752": true, "M100": true}
	dbInvalidOrderCodes  = map[string]bool{"2706": true, "3180": true, "3181": true}
	dbOrderNotFoundCodes = map[string]bool{"3056": true, "3416": true}
)

type DbClient struct {
	appKey, appSecret string
	baseURL           string
	macAddress        string
	marketDivCode     string
	environment       TradingEnvironment
	http              *http.Client
	limiter           *rateLimiter
	tokenMu           sync.Mutex
	token             string
	tokenExpires      time.Time
	call              func(path string, body map[string]any) (map[string]any, error)
}

// NewDbClient - 운영/모의는 같은 호스트, 모의투자용 키로만 분기된다. SetEnvironment 는 엔진의 실전 게이트용 선언이다.
func NewDbClient(appKey, appSecret string) *DbClient {
	c := &DbClient{
		appKey: appKey, appSecret: appSecret, baseURL: DbBaseURL, marketDivCode: "J", environment: Paper,
		http:    &http.Client{Timeout: 30 * time.Second},
		limiter: newRateLimiter(500*time.Millisecond, 4, func(a int) time.Duration { return time.Duration(1<<(a-1)) * time.Second }),
	}
	c.call = c.request
	return c
}

func (c *DbClient) SetBaseURL(u string) *DbClient                   { c.baseURL = u; return c }
func (c *DbClient) SetMacAddress(m string) *DbClient                { c.macAddress = m; return c }
func (c *DbClient) SetEnvironment(env TradingEnvironment) *DbClient { c.environment = env; return c }
func (c *DbClient) Environment() TradingEnvironment                 { return c.environment }
func (c *DbClient) SetThrottle(d time.Duration) *DbClient {
	c.limiter = newRateLimiter(d, 4, func(a int) time.Duration { return time.Duration(1<<(a-1)) * time.Second })
	return c
}

func (c *DbClient) Capabilities() BrokerCapabilities {
	return BrokerCapabilities{
		BrokerID: "db", Market: "KRX", Currency: "KRW",
		CandleIntervals: map[CandleInterval]bool{Day1: true},
		ClientOrderID:   false, NativeBracket: false, FractionalShares: false, ServerOpenOrders: true,
		Environments: map[TradingEnvironment]bool{Paper: true, Live: true},
	}
}

// DbNormalizeCode - 계좌·주문계 IsuNo 는 A005930 형태일 수 있다 → 6자리 코드.
func DbNormalizeCode(raw any) string {
	text := strings.TrimSpace(str(raw))
	if len(text) == 7 && text[0] == 'A' {
		return text[1:]
	}
	return text
}

func dbSide(row map[string]any) OrderSide {
	if strings.TrimSpace(str(row["BnsTpCode"])) == "2" {
		return Buy
	}
	return Sell
}

func dbRemaining(row map[string]any) decimal.Decimal {
	if r := nhNum(row["MrcAbleQty"]); r != nil {
		return *r
	}
	return nhVal(row["OrdQty"]).Sub(nhVal(row["AllExecQty"])).Sub(nhVal(row["MrcQty"]))
}

// ------------------------------------------------------------------- market

func (c *DbClient) GetQuotes(symbols []string) ([]Quote, error) {
	caps := c.Capabilities()
	quotes := make([]Quote, 0, len(symbols))
	for _, symbol := range symbols {
		code, err := caps.SymbolCode(symbol)
		if err != nil {
			return nil, err
		}
		body, err := c.call("/api/v1/quote/kr-stock/inquiry/price", map[string]any{"InputCondMrktDivCode": c.marketDivCode, "InputIscd1": code})
		if err != nil {
			return nil, err
		}
		out := obj(body, "Out")
		quotes = append(quotes, Quote{
			Symbol: symbol, Price: nhVal(out["Prpr"]), BidPrice: nhPositive(out["Bidp1"]), AskPrice: nhPositive(out["Askp1"]),
			Volume: nhVal(out["AcmlVol"]).IntPart(), Change: nhNum(out["PrdyVrss"]), ChangeRate: nhPct(out["PrdyCtrt"]), Timestamp: time.Now(),
		})
	}
	return quotes, nil
}

func (c *DbClient) GetCandles(symbol string, interval CandleInterval, limit int) ([]Candle, error) {
	if interval != Day1 {
		return nil, fmt.Errorf("DB 어댑터는 일봉(1d)만 지원합니다")
	}
	code, err := c.Capabilities().SymbolCode(symbol)
	if err != nil {
		return nil, err
	}
	count := limit
	if count <= 0 {
		count = 30
	}
	today := time.Now().In(kst)
	start := today.AddDate(0, 0, -(count*16/10 + 10)) // 휴장일 여유
	body, err := c.call("/api/v1/quote/kr-chart/day", map[string]any{
		"InputOrgAdjPrc": "1", "InputCondMrktDivCode": c.marketDivCode, "InputIscd1": code,
		"InputDate1": start.Format("20060102"), "InputDate2": today.Format("20060102"),
	})
	if err != nil {
		return nil, err
	}
	candles := make([]Candle, 0)
	for _, r := range rows(body, "Out") {
		ts, err := time.ParseInLocation("20060102", strings.TrimSpace(str(r["Date"])), kst)
		if err != nil {
			continue
		}
		vol := nhNum(r["CntgVol"])
		if vol == nil {
			vol = nhNum(r["AcmlVol"]) // 2024 샘플은 AcmlVol
		}
		var volume int64
		if vol != nil {
			volume = vol.IntPart()
		}
		candles = append(candles, Candle{
			Timestamp: ts, Open: nhVal(r["Oprc"]), High: nhVal(r["Hprc"]), Low: nhVal(r["Lprc"]), Close: nhVal(r["Prpr"]), Volume: volume,
		})
	}
	sortCandles(candles)
	if len(candles) > count {
		candles = candles[len(candles)-count:]
	}
	return candles, nil
}

func (c *DbClient) GetCalendar() ([]MarketDay, error) { return krxCalendar(31), nil }

// ------------------------------------------------------------------ account

func (c *DbClient) accountID() string { return "db-" + strings.ToLower(string(c.environment)) }

func (c *DbClient) GetAccount() (Account, error) {
	body, err := c.balance()
	if err != nil {
		return Account{}, err
	}
	out := obj(body, "Out")
	portfolio := nhVal(out["DpsastAmt"]) // 예탁자산 = 현금 + 평가
	cash := portfolio.Sub(nhVal(out["TotEvalAmt"]))
	if d := nhNum(out["Dps2"]); d != nil {
		cash = *d
	}
	return Account{AccountID: c.accountID(), Currency: "KRW", Cash: cash, PortfolioValue: portfolio, Status: "ACTIVE"}, nil
}

func (c *DbClient) GetHoldings() ([]Holding, error) {
	body, err := c.balance()
	if err != nil {
		return nil, err
	}
	holdings := make([]Holding, 0)
	for _, row := range rows(body, "Out1") {
		qty := nhNum(row["BalQty0"])
		if qty == nil {
			qty = nhNum(row["BalQty"])
		}
		if qty == nil || !qty.IsPositive() {
			continue
		}
		holdings = append(holdings, Holding{
			Symbol: DbNormalizeCode(row["IsuNo"]), Quantity: *qty, AvgEntryPrice: nhVal(row["ExecPrc"]),
			CurrentPrice: nhNum(row["NowPrc"]), MarketValue: nhNum(row["EvalAmt"]), UnrealizedPnl: nhNum(row["EvalPnlAmt"]), UnrealizedPnlRate: nhPct(row["Ernrat"]),
		})
	}
	return holdings, nil
}

func (c *DbClient) GetBuyingPower() (decimal.Decimal, error) {
	body, err := c.call("/api/v1/trading/kr-stock/inquiry/acnt-deposit", map[string]any{})
	if err != nil {
		return decimal.Zero, err
	}
	out := obj(body, "Out1")
	if d := nhNum(out["DpsBalAmt"]); d != nil {
		return *d, nil
	}
	return nhVal(out["WthdwAbleAmt"]), nil
}

// ------------------------------------------------------------------- orders

func (c *DbClient) CreateOrder(request CreateOrderRequest) (Order, error) {
	code, err := c.Capabilities().SymbolCode(request.Symbol)
	if err != nil {
		return Order{}, err
	}
	isLimit := request.OrderType == Limit
	var price int64
	if isLimit && request.LimitPrice != nil {
		price = KrxTickRound(*request.LimitPrice).IntPart() // KRX 호가단위 보정(2706)
	}
	side := "1"
	if request.Side == Buy {
		side = "2"
	}
	ptn := "03"
	if isLimit {
		ptn = "00"
	}
	res, err := c.call("/api/v1/trading/kr-stock/order", map[string]any{
		"IsuNo": code, "TrchNo": 1, "OrdQty": request.Quantity.IntPart(), "OrdPrc": price,
		"BnsTpCode": side, "OrdprcPtnCode": ptn, "MgntrnCode": "000", "LoanDt": "00000000", "OrdCndiTpCode": "0",
	})
	if err != nil {
		return Order{}, err
	}
	orderID := strings.TrimSpace(str(obj(res, "Out")["OrdNo"]))
	if orderID == "" {
		return Order{}, &BrokerAPIError{200, "", "DB 주문 응답에 OrdNo 가 없습니다"}
	}
	zero := decimal.Zero
	now := time.Now()
	return Order{
		OrderID: orderID, Status: Submitted, Symbol: code, Side: request.Side, OrderType: request.OrderType,
		Quantity: &request.Quantity, LimitPrice: request.LimitPrice, FilledQuantity: &zero, ClientOrderID: request.ClientOrderID, SubmittedAt: &now,
	}, nil
}

func (c *DbClient) GetOrders() ([]Order, error) {
	rowsList, err := c.historyRows()
	if err != nil {
		return nil, err
	}
	orders := make([]Order, 0)
	for _, r := range rowsList {
		if o := c.toOrder(r); o.Status.IsOpen() {
			orders = append(orders, o)
		}
	}
	return orders, nil
}

func (c *DbClient) GetOrder(orderID string) (Order, error) {
	rowsList, err := c.historyRows()
	if err != nil {
		return Order{}, err
	}
	for _, r := range rowsList {
		if nhSameNo(r["OrdNo"], orderID) {
			return c.toOrder(r), nil
		}
	}
	return Order{OrderID: orderID, Status: Canceled}, nil
}

func (c *DbClient) CancelOrder(orderID string) (Order, error) {
	rowsList, err := c.historyRows()
	if err != nil {
		return Order{}, err
	}
	var row map[string]any
	for _, r := range rowsList {
		if nhSameNo(r["OrdNo"], orderID) {
			row = r
		}
	}
	if row == nil {
		return Order{}, newOrderNotFoundError("order-not-found", "DB 당일 주문에서 찾을 수 없습니다: "+orderID)
	}
	orgNo, _ := strconv.ParseInt(strings.TrimLeft(orderID, "0"), 10, 64)
	if _, err := c.call("/api/v1/trading/kr-stock/order-cancel", map[string]any{
		"OrgOrdNo": orgNo, "IsuNo": DbNormalizeCode(row["IsuNo"]), "OrdQty": dbRemaining(row).IntPart(),
	}); err != nil {
		return Order{}, err
	}
	now := time.Now()
	return Order{OrderID: orderID, Status: PendingCancel, CanceledAt: &now}, nil
}

func (c *DbClient) GetFills() ([]Fill, error) {
	rowsList, err := c.historyRows()
	if err != nil {
		return nil, err
	}
	fills := make([]Fill, 0)
	for _, r := range rowsList {
		qty := nhNum(r["AllExecQty"])
		if qty == nil || !qty.IsPositive() {
			continue
		}
		fills = append(fills, Fill{OrderID: strings.TrimSpace(str(r["OrdNo"])), Symbol: DbNormalizeCode(r["IsuNo"]), Side: dbSide(r), Quantity: qty, Price: nhNum(r["AvrExecPrc"])})
	}
	return fills, nil
}

// ----------------------------------------------------------------- internal

func (c *DbClient) balance() (map[string]any, error) {
	return c.call("/api/v1/trading/kr-stock/inquiry/balance", map[string]any{"QryTpCode0": "0"})
}

func (c *DbClient) historyRows() ([]map[string]any, error) {
	body, err := c.call("/api/v1/trading/kr-stock/inquiry/transaction-history", map[string]any{
		"SorTpYn": "2", "ExecYn": "0", "TrdMktCode": "0", "BnsTpCode": "0", "IsuTpCode": "0", "QryTp": "0",
	})
	if err != nil {
		return nil, err
	}
	return rows(body, "Out1"), nil
}

func (c *DbClient) toOrder(row map[string]any) Order {
	ordQty := nhVal(row["OrdQty"])
	filled := nhVal(row["AllExecQty"])
	remaining := dbRemaining(row)
	trx := strings.TrimSpace(str(row["OrdTrxPtnCode"]))
	var status OrderStatus
	switch {
	case trx == "9":
		status = PendingCancel
	case trx == "8":
		status = Canceled
	case remaining.IsPositive() && filled.IsPositive():
		status = PartiallyFilled
	case remaining.IsPositive():
		status = Submitted
	case filled.IsPositive() && filled.GreaterThanOrEqual(ordQty):
		status = Filled
	case filled.IsPositive():
		status = PartiallyFilled
	default:
		status = Canceled
	}
	orderType := Limit
	if strings.TrimSpace(str(row["OrdprcPtnCode"])) == "03" {
		orderType = Market
	}
	return Order{
		OrderID: strings.TrimSpace(str(row["OrdNo"])), Status: status, Symbol: DbNormalizeCode(row["IsuNo"]), Side: dbSide(row),
		OrderType: orderType, Quantity: &ordQty, LimitPrice: nhPositive(row["OrdPrc"]), FilledQuantity: &filled, AvgFillPrice: nhPositive(row["AvrExecPrc"]),
	}
}

func (c *DbClient) request(path string, body map[string]any) (map[string]any, error) {
	return c.limiter.execute("DB "+path, func() (map[string]any, error) { return c.requestOnce(path, body) })
}

func (c *DbClient) requestOnce(path string, body map[string]any) (map[string]any, error) {
	token, err := c.getToken()
	if err != nil {
		return nil, err
	}
	headers := map[string]string{
		"Content-Type": "application/json; charset=utf-8", "authorization": "Bearer " + token, "cont_yn": "N", "cont_key": "",
	}
	if c.macAddress != "" {
		headers["mac_address"] = c.macAddress
	}
	payload, _ := json.Marshal(map[string]any{"In": body})
	status, parsed, resHeaders, err := httpJSONHeaders(c.http, "POST", c.baseURL+path, headers, payload)
	if err != nil {
		return nil, err
	}
	code := strings.TrimSpace(str(parsed["rsp_cd"]))
	rspMsg := str(parsed["rsp_msg"])
	msg := strings.TrimSpace(fmt.Sprintf("DB(%s) [%s] %s", path, code, rspMsg))
	if status < 200 || status >= 300 || code != "00000" {
		retryAfter, _ := strconv.ParseFloat(strings.TrimSpace(resHeaders.Get("Retry-After")), 64)
		switch {
		case code == "IGW00201" || status == 429:
			return nil, newRateLimitErrorWithRetryAfter(status, code, msg, retryAfter)
		case dbAuthCodes[code] || status == 401:
			return nil, newAuthError(status, code, msg)
		case dbMarketClosedCodes[code]:
			return nil, newMarketClosedError(status, code, msg)
		case dbInsufficientCodes[code] || strings.Contains(rspMsg, "부족"):
			return nil, &InsufficientFundsError{BrokerAPIError{status, code, msg}}
		case dbInvalidOrderCodes[code]:
			return nil, &InvalidOrderError{BrokerAPIError{status, code, msg}}
		case dbOrderNotFoundCodes[code]:
			return nil, newOrderNotFoundError(code, msg)
		default:
			return nil, &BrokerAPIError{status, code, msg}
		}
	}
	return parsed, nil
}

func (c *DbClient) getToken() (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.token != "" && time.Now().Before(c.tokenExpires.Add(-10*time.Minute)) {
		return c.token, nil
	}
	c.limiter.throttle.wait()
	form := url.Values{"grant_type": {"client_credentials"}, "appkey": {c.appKey}, "appsecretkey": {c.appSecret}, "scope": {"oob"}} // JSON/appsecret 은 IGW00133
	status, body, err := httpJSON(c.http, "POST", c.baseURL+"/oauth2/token",
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, []byte(form.Encode()))
	if err != nil {
		return "", err
	}
	token := str(body["access_token"])
	if status != 200 || token == "" {
		code := str(body["rsp_cd"])
		if code == "" {
			code = str(body["error"])
		}
		return "", newAuthError(status, code, fmt.Sprintf("DB 토큰 발급 실패(%s): %s (발급은 1분당 1회 제한)", code, str(body["rsp_msg"])+str(body["error_description"])))
	}
	c.token = token
	c.tokenExpires = time.Now().Add(time.Duration(nhVal(body["expires_in"]).IntPart()) * time.Second)
	if c.tokenExpires.Before(time.Now().Add(time.Minute)) {
		c.tokenExpires = time.Now().Add(24 * time.Hour)
	}
	return c.token, nil
}
