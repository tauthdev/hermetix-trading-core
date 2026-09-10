package hermetix

// LS증권(구 이베스트투자증권) OPEN API 어댑터.
//
// ⚠️ 문서 기반 구현 (실측 전) — 공식 포털 TR 문서와 커뮤니티 카탈로그(krsec, LsApiHelper, k-ebest-im, 공식 샘플)에서
// 엔드포인트·TR 코드·필드명을 역추적했다. 모의서버 실측 전까지 상태는 "미검증".
//   - 모든 API 는 POST, 경로는 기능군(/stock/market-data, /stock/chart, /stock/accno, /stock/order)이고 TR 은 tr_cd 헤더로 고른다
//   - 본문 {"<TR>InBlock": {...}} 또는 {"<TR>InBlock1": {...}}, 응답 rsp_cd("00000" 성공)/rsp_msg + OutBlock 들. 계좌번호는 토큰에 바인딩
//   - 실전/모의 같은 호스트(모의 appkey 로 라우팅). 모의 주문은 IsuNo 에 A 접두 필수 → 항상 A+코드
//   - HTTP 200 + rsp_cd != 00000 이 업무 오류. TR 별 TPS(시세 3, 차트 1, 계좌 2, 예수금 1, 주문 10) → 전역 0.5s + 차트 전용 1.1s 쓰로틀
//
// 미확인(실측 필요): 잔고 expcode 의 A 접두, sign 코드 의미(4·5 하락 가정), 응답 숫자 타입, 장 마감 코드(메시지 판단), medosu 표기

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

const LsBaseURL = "https://openapi.ls-sec.co.kr:8080"

var (
	lsAuthCodes    = map[string]bool{"IGW00121": true, "IGW00123": true}
	lsFallingSigns = map[string]bool{"4": true, "5": true}
)

type LsClient struct {
	appKey, appSecret string
	baseURL           string
	macAddress        string
	exchGubun         string
	environment       TradingEnvironment
	http              *http.Client
	limiter           *rateLimiter
	chartLimiter      *rateLimiter
	tokenMu           sync.Mutex
	token             string
	tokenExpires      time.Time
	call              func(path, trCd, inBlock string, body map[string]any) (map[string]any, error)
}

// NewLsClient - 실전/모의는 같은 호스트, 모의투자용 appkey 로만 분기된다. SetEnvironment 는 엔진의 실전 게이트용 선언이다.
func NewLsClient(appKey, appSecret string) *LsClient {
	c := &LsClient{
		appKey: appKey, appSecret: appSecret, baseURL: LsBaseURL, environment: Paper,
		http:         &http.Client{Timeout: 30 * time.Second},
		limiter:      newRateLimiter(500*time.Millisecond, 3, func(a int) time.Duration { return time.Duration(a) * time.Second }),
		chartLimiter: newRateLimiter(1100*time.Millisecond, 0, func(int) time.Duration { return 0 }),
	}
	c.call = c.request
	return c
}

func (c *LsClient) SetBaseURL(u string) *LsClient                   { c.baseURL = u; return c }
func (c *LsClient) SetMacAddress(m string) *LsClient                { c.macAddress = m; return c }
func (c *LsClient) SetExchGubun(g string) *LsClient                 { c.exchGubun = g; return c }
func (c *LsClient) SetEnvironment(env TradingEnvironment) *LsClient { c.environment = env; return c }
func (c *LsClient) Environment() TradingEnvironment                 { return c.environment }
func (c *LsClient) SetThrottle(d time.Duration) *LsClient {
	c.limiter = newRateLimiter(d, 3, func(a int) time.Duration { return time.Duration(a) * time.Second })
	return c
}

// SetChartThrottle - 차트 TR(t8410) 전용 간격. 초당 1건 한도라 기본 1.1s.
func (c *LsClient) SetChartThrottle(d time.Duration) *LsClient {
	c.chartLimiter = newRateLimiter(d, 0, func(int) time.Duration { return 0 })
	return c
}

func (c *LsClient) Capabilities() BrokerCapabilities {
	return BrokerCapabilities{
		BrokerID: "ls", Market: "KRX", Currency: "KRW",
		CandleIntervals: map[CandleInterval]bool{Day1: true},
		ClientOrderID:   false, NativeBracket: false, FractionalShares: false, ServerOpenOrders: true,
		Environments: map[TradingEnvironment]bool{Paper: true, Live: true},
	}
}

// LsNormalizeCode - 계좌 TR 의 종목코드는 A005930 형태(추정) → 6자리 코드.
func LsNormalizeCode(raw any) string {
	text := strings.TrimSpace(str(raw))
	if len(text) == 7 && text[0] == 'A' {
		return text[1:]
	}
	return text
}

func lsSide(row map[string]any) OrderSide {
	text := strings.TrimSpace(str(row["medosu"]))
	if strings.Contains(text, "매수") || text == "2" {
		return Buy
	}
	return Sell
}

func lsRemaining(row map[string]any) decimal.Decimal {
	if r := nhNum(row["ordrem"]); r != nil {
		return *r
	}
	return nhVal(row["qty"]).Sub(nhVal(row["cheqty"]))
}

// ------------------------------------------------------------------- market

func (c *LsClient) GetQuotes(symbols []string) ([]Quote, error) {
	caps := c.Capabilities()
	quotes := make([]Quote, 0, len(symbols))
	for _, symbol := range symbols {
		code, err := caps.SymbolCode(symbol)
		if err != nil {
			return nil, err
		}
		body, err := c.call("/stock/market-data", "t1102", "t1102InBlock", map[string]any{"shcode": code, "exchgubun": c.exchGubun})
		if err != nil {
			return nil, err
		}
		out := obj(body, "t1102OutBlock")
		change := nhNum(out["change"])
		rate := nhNum(out["diff"])
		if lsFallingSigns[strings.TrimSpace(str(out["sign"]))] {
			if change != nil && change.IsPositive() {
				n := change.Neg()
				change = &n
			}
			if rate != nil && rate.IsPositive() {
				n := rate.Neg()
				rate = &n
			}
		}
		var changeRate *decimal.Decimal
		if rate != nil {
			r := rate.Div(hundred)
			changeRate = &r
		}
		quotes = append(quotes, Quote{
			Symbol: symbol, Price: nhVal(out["price"]), Volume: nhVal(out["volume"]).IntPart(),
			Change: change, ChangeRate: changeRate, Timestamp: time.Now(),
		})
	}
	return quotes, nil
}

func (c *LsClient) GetCandles(symbol string, interval CandleInterval, limit int) ([]Candle, error) {
	if interval != Day1 {
		return nil, fmt.Errorf("LS 어댑터는 일봉(1d)만 지원합니다")
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
	start := today.AddDate(0, 0, -(count*16/10 + 10))
	body, err := c.chartLimiter.execute("LS t8410", func() (map[string]any, error) {
		return c.call("/stock/chart", "t8410", "t8410InBlock", map[string]any{
			"shcode": code, "gubun": "2", "qrycnt": count, "sdate": start.Format("20060102"), "edate": today.Format("20060102"),
			"cts_date": "", "comp_yn": "N", "sujung": "Y",
		})
	})
	if err != nil {
		return nil, err
	}
	candles := make([]Candle, 0)
	for _, r := range rows(body, "t8410OutBlock1") {
		ts, err := time.ParseInLocation("20060102", strings.TrimSpace(str(r["date"])), kst)
		if err != nil {
			continue
		}
		candles = append(candles, Candle{
			Timestamp: ts, Open: nhVal(r["open"]), High: nhVal(r["high"]), Low: nhVal(r["low"]), Close: nhVal(r["close"]),
			Volume: nhVal(r["jdiff_vol"]).IntPart(),
		})
	}
	sortCandles(candles)
	if len(candles) > count {
		candles = candles[len(candles)-count:]
	}
	return candles, nil
}

func (c *LsClient) GetCalendar() ([]MarketDay, error) { return krxCalendar(31), nil }

// ------------------------------------------------------------------ account

func (c *LsClient) accountID() string { return "ls-" + strings.ToLower(string(c.environment)) }

func (c *LsClient) GetAccount() (Account, error) {
	body, err := c.balance()
	if err != nil {
		return Account{}, err
	}
	s := obj(body, "t0424OutBlock")
	cash := nhVal(s["sunamt1"]) // 추정 D2 예수금
	portfolio := nhVal(s["sunamt"])
	if !portfolio.IsPositive() {
		portfolio = cash.Add(nhVal(s["tappamt"]))
	}
	return Account{AccountID: c.accountID(), Currency: "KRW", Cash: cash, PortfolioValue: portfolio, Status: "ACTIVE"}, nil
}

func (c *LsClient) GetHoldings() ([]Holding, error) {
	body, err := c.balance()
	if err != nil {
		return nil, err
	}
	holdings := make([]Holding, 0)
	for _, row := range rows(body, "t0424OutBlock1") {
		qty := nhNum(row["janqty"])
		if qty == nil || !qty.IsPositive() {
			continue
		}
		holdings = append(holdings, Holding{
			Symbol: LsNormalizeCode(row["expcode"]), Quantity: *qty, AvgEntryPrice: nhVal(row["pamt"]),
			CurrentPrice: nhNum(row["price"]), MarketValue: nhNum(row["appamt"]), UnrealizedPnl: nhNum(row["dtsunik"]), UnrealizedPnlRate: nhPct(row["sunikrt"]),
		})
	}
	return holdings, nil
}

func (c *LsClient) GetBuyingPower() (decimal.Decimal, error) {
	body, err := c.call("/stock/accno", "CSPAQ12200", "CSPAQ12200InBlock1", map[string]any{"BalCreTp": "0"})
	if err != nil {
		return decimal.Zero, err
	}
	out := obj(body, "CSPAQ12200OutBlock2")
	if d := nhNum(out["MnyOrdAbleAmt"]); d != nil {
		return *d, nil
	}
	return nhVal(out["Dps"]), nil
}

// ------------------------------------------------------------------- orders

func (c *LsClient) CreateOrder(request CreateOrderRequest) (Order, error) {
	code, err := c.Capabilities().SymbolCode(request.Symbol)
	if err != nil {
		return Order{}, err
	}
	isLimit := request.OrderType == Limit
	var price int64
	if isLimit && request.LimitPrice != nil {
		price = KrxTickRound(*request.LimitPrice).IntPart() // KRX 호가단위 보정
	}
	side := "1"
	if request.Side == Buy {
		side = "2"
	}
	ptn := "03"
	if isLimit {
		ptn = "00"
	}
	res, err := c.call("/stock/order", "CSPAT00601", "CSPAT00601InBlock1", map[string]any{
		"IsuNo": "A" + code, "OrdQty": request.Quantity.IntPart(), "OrdPrc": price,
		"BnsTpCode": side, "OrdprcPtnCode": ptn, "MgntrnCode": "000", "LoanDt": "", "OrdCndiTpCode": "0",
	})
	if err != nil {
		return Order{}, err
	}
	orderID := strings.TrimSpace(str(obj(res, "CSPAT00601OutBlock2")["OrdNo"]))
	if orderID == "" || orderID == "0" {
		return Order{}, &BrokerAPIError{200, "", "LS 주문 응답에 OrdNo 가 없습니다"}
	}
	zero := decimal.Zero
	now := time.Now()
	return Order{
		OrderID: orderID, Status: Submitted, Symbol: code, Side: request.Side, OrderType: request.OrderType,
		Quantity: &request.Quantity, LimitPrice: request.LimitPrice, FilledQuantity: &zero, ClientOrderID: request.ClientOrderID, SubmittedAt: &now,
	}, nil
}

func (c *LsClient) GetOrders() ([]Order, error) {
	rowsList, err := c.orderRows()
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

func (c *LsClient) GetOrder(orderID string) (Order, error) {
	rowsList, err := c.orderRows()
	if err != nil {
		return Order{}, err
	}
	for _, r := range rowsList {
		if nhSameNo(r["ordno"], orderID) {
			return c.toOrder(r), nil
		}
	}
	return Order{OrderID: orderID, Status: Canceled}, nil
}

func (c *LsClient) CancelOrder(orderID string) (Order, error) {
	rowsList, err := c.orderRows()
	if err != nil {
		return Order{}, err
	}
	var row map[string]any
	for _, r := range rowsList {
		if nhSameNo(r["ordno"], orderID) {
			row = r
		}
	}
	if row == nil {
		return Order{}, newOrderNotFoundError("order-not-found", "LS 당일 주문에서 찾을 수 없습니다: "+orderID)
	}
	orgNo, _ := strconv.ParseInt(strings.TrimLeft(orderID, "0"), 10, 64)
	if _, err := c.call("/stock/order", "CSPAT00801", "CSPAT00801InBlock1", map[string]any{
		"OrgOrdNo": orgNo, "IsuNo": "A" + LsNormalizeCode(row["expcode"]), "OrdQty": lsRemaining(row).IntPart(),
	}); err != nil {
		return Order{}, err
	}
	now := time.Now()
	return Order{OrderID: orderID, Status: PendingCancel, CanceledAt: &now}, nil
}

func (c *LsClient) GetFills() ([]Fill, error) {
	rowsList, err := c.orderRows()
	if err != nil {
		return nil, err
	}
	fills := make([]Fill, 0)
	for _, r := range rowsList {
		qty := nhNum(r["cheqty"])
		if qty == nil || !qty.IsPositive() {
			continue
		}
		fills = append(fills, Fill{OrderID: strings.TrimSpace(str(r["ordno"])), Symbol: LsNormalizeCode(r["expcode"]), Side: lsSide(r), Quantity: qty, Price: nhPositive(r["cheprice"])})
	}
	return fills, nil
}

// ----------------------------------------------------------------- internal

func (c *LsClient) balance() (map[string]any, error) {
	return c.call("/stock/accno", "t0424", "t0424InBlock", map[string]any{"prcgb": "1", "chegb": "2", "dangb": "0", "charge": "1", "cts_expcode": ""})
}

func (c *LsClient) orderRows() ([]map[string]any, error) {
	body, err := c.call("/stock/accno", "t0425", "t0425InBlock", map[string]any{"expcode": "", "chegb": "0", "medosu": "0", "sortgb": "1", "cts_ordno": ""})
	if err != nil {
		return nil, err
	}
	return rows(body, "t0425OutBlock1"), nil
}

func (c *LsClient) toOrder(row map[string]any) Order {
	qty := nhVal(row["qty"])
	filled := nhVal(row["cheqty"])
	remaining := lsRemaining(row)
	text := str(row["status"])
	var status OrderStatus
	switch {
	case strings.Contains(text, "취소") && remaining.IsPositive():
		status = PendingCancel
	case remaining.IsPositive() && filled.IsPositive():
		status = PartiallyFilled
	case remaining.IsPositive():
		status = Submitted
	case filled.IsPositive() && filled.GreaterThanOrEqual(qty):
		status = Filled
	case filled.IsPositive():
		status = PartiallyFilled
	case strings.Contains(text, "거부"):
		status = Rejected
	default:
		status = Canceled
	}
	orderType := Limit
	if strings.TrimSpace(str(row["hogagb"])) == "03" {
		orderType = Market
	}
	return Order{
		OrderID: strings.TrimSpace(str(row["ordno"])), Status: status, Symbol: LsNormalizeCode(row["expcode"]), Side: lsSide(row),
		OrderType: orderType, Quantity: &qty, LimitPrice: nhPositive(row["price"]), FilledQuantity: &filled, AvgFillPrice: nhPositive(row["cheprice"]),
	}
}

func (c *LsClient) request(path, trCd, inBlock string, body map[string]any) (map[string]any, error) {
	return c.limiter.execute("LS "+trCd, func() (map[string]any, error) { return c.requestOnce(path, trCd, inBlock, body) })
}

func (c *LsClient) requestOnce(path, trCd, inBlock string, body map[string]any) (map[string]any, error) {
	token, err := c.getToken()
	if err != nil {
		return nil, err
	}
	headers := map[string]string{
		"Content-Type": "application/json; charset=utf-8", "authorization": "Bearer " + token, "tr_cd": trCd, "tr_cont": "N", "tr_cont_key": "",
	}
	if c.macAddress != "" {
		headers["mac_address"] = c.macAddress
	}
	payload, _ := json.Marshal(map[string]any{inBlock: body})
	status, parsed, resHeaders, err := httpJSONHeaders(c.http, "POST", c.baseURL+path, headers, payload)
	if err != nil {
		return nil, err
	}
	code := strings.TrimSpace(str(parsed["rsp_cd"]))
	if code == "" {
		code = strings.TrimSpace(str(parsed["error_code"]))
	}
	rspMsg := str(parsed["rsp_msg"])
	if rspMsg == "" {
		rspMsg = str(parsed["error_description"])
	}
	_, hasRsp := parsed["rsp_cd"]
	msg := strings.TrimSpace(fmt.Sprintf("LS(%s) [%s] %s", trCd, code, rspMsg))
	if status < 200 || status >= 300 || (hasRsp && code != "00000") {
		retryAfter, _ := strconv.ParseFloat(strings.TrimSpace(resHeaders.Get("Retry-After")), 64)
		switch {
		case code == "IGW00201" || status == 429:
			return nil, newRateLimitErrorWithRetryAfter(status, code, msg, retryAfter)
		case lsAuthCodes[code] || status == 401:
			return nil, newAuthError(status, code, msg)
		case containsAny(rspMsg, "장종료", "장운영", "장 마감", "장마감"):
			return nil, newMarketClosedError(status, code, msg)
		case strings.Contains(rspMsg, "부족"):
			return nil, &InsufficientFundsError{BrokerAPIError{status, code, msg}}
		case containsAny(rspMsg, "호가", "단위"):
			return nil, &InvalidOrderError{BrokerAPIError{status, code, msg}}
		case containsAny(rspMsg, "주문번호", "원주문"):
			return nil, newOrderNotFoundError(code, msg)
		default:
			return nil, &BrokerAPIError{status, code, msg}
		}
	}
	return parsed, nil
}

func (c *LsClient) getToken() (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.token != "" && time.Now().Before(c.tokenExpires.Add(-10*time.Minute)) {
		return c.token, nil
	}
	c.limiter.throttle.wait()
	form := url.Values{"grant_type": {"client_credentials"}, "appkey": {c.appKey}, "appsecretkey": {c.appSecret}, "scope": {"oob"}}
	status, body, err := httpJSON(c.http, "POST", c.baseURL+"/oauth2/token",
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, []byte(form.Encode()))
	if err != nil {
		return "", err
	}
	token := str(body["access_token"])
	if status != 200 || token == "" {
		code := str(body["error_code"])
		if code == "" {
			code = str(body["rsp_cd"])
		}
		return "", newAuthError(status, code, fmt.Sprintf("LS 토큰 발급 실패(%s): %s", code, str(body["error_description"])+str(body["rsp_msg"])))
	}
	c.token = token
	c.tokenExpires = time.Now().Add(time.Duration(nhVal(body["expires_in"]).IntPart()) * time.Second)
	if c.tokenExpires.Before(time.Now().Add(time.Minute)) {
		c.tokenExpires = time.Now().Add(24 * time.Hour)
	}
	return c.token, nil
}

func containsAny(text string, keywords ...string) bool {
	for _, k := range keywords {
		if strings.Contains(text, k) {
			return true
		}
	}
	return false
}
