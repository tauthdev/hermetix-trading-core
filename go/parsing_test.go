package hermetix

// 어댑터 정규화 검증 - 실측 골든 픽스처 재생 (Kotlin/Python/JS 와 동일 정답지).

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func stubCalls(responses []map[string]any) func() map[string]any {
	i := 0
	return func() map[string]any {
		r := responses[i]
		i++
		return r
	}
}

// v1.3: outcome=OK 만 공통 모델로, 등락률 % → 비율, KST 시각 파싱
func TestNextQuoteParsing(t *testing.T) {
	c := NewNextClient("k", "s")
	c.call = func(method, path string, account bool, jsonBody map[string]any) (map[string]any, error) {
		return map[string]any{"quotes": []any{
			map[string]any{
				"symbol": "AAPL", "outcome": "OK", "session": "REGULAR", "requestedAt": "2026-09-10T23:10:00+09:00",
				"price": "308.91", "previousClose": "333.43", "change": "-24.52", "changeRate": "-7.3539",
				"bidPrice": nil, "askPrice": nil, "volume": "132756799", "lastTradeAt": "2026-09-10T23:09:58+09:00",
			},
			map[string]any{"symbol": "NOPE", "outcome": "NOT_FOUND", "session": "CLOSED", "requestedAt": "2026-09-10T23:10:00+09:00"},
		}}, nil
	}
	quotes, err := c.GetQuotes([]string{"AAPL", "NOPE"})
	if err != nil {
		t.Fatal(err)
	}
	if len(quotes) != 1 || quotes[0].Symbol != "AAPL" {
		t.Fatalf("NOT_FOUND 종목은 제외돼야 한다: %+v", quotes)
	}
	if quotes[0].Price.String() != "308.91" {
		t.Fatalf("price = %s", quotes[0].Price)
	}
	if quotes[0].BidPrice != nil {
		t.Fatal("bidPrice should be nil")
	}
	if quotes[0].ChangeRate.String() != "-0.073539" {
		t.Fatalf("changeRate = %s", quotes[0].ChangeRate)
	}
	if quotes[0].Volume != 132756799 {
		t.Fatalf("volume = %d", quotes[0].Volume)
	}
	if got := quotes[0].Timestamp.UTC().Format(time.RFC3339); got != "2026-09-10T14:09:58Z" {
		t.Fatalf("timestamp = %s", got)
	}
}

func TestNextCandleTimeWithoutOffsetIsKST(t *testing.T) {
	c := NewNextClient("k", "s")
	c.call = func(method, path string, account bool, jsonBody map[string]any) (map[string]any, error) {
		return map[string]any{"symbol": "AAPL", "interval": "1d", "candles": []any{map[string]any{
			"time": "2026-09-09T22:30:00", "session": "REGULAR", "open": "339.73", "high": "344.56",
			"low": "337.35", "close": "338.19", "volume": "56298904",
		}}}, nil
	}
	candles, err := c.GetCandles("AAPL", Day1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got := candles[0].Timestamp.UTC().Format(time.RFC3339); got != "2026-09-09T13:30:00Z" {
		t.Fatalf("timestamp = %s", got)
	}
}

func TestNextCalendarKSTToNewYork(t *testing.T) {
	c := NewNextClient("k", "s")
	c.call = func(method, path string, account bool, jsonBody map[string]any) (map[string]any, error) {
		return map[string]any{"calendar": []any{
			map[string]any{"date": "2026-09-10", "status": "OPEN", "holidayName": nil, "sessions": []any{
				map[string]any{"type": "PRE", "open": "2026-09-10T17:00:00+09:00", "close": "2026-09-10T22:30:00+09:00"},
				map[string]any{"type": "REGULAR", "open": "2026-09-10T22:30:00+09:00", "close": "2026-09-11T05:00:00+09:00"},
			}},
			map[string]any{"date": "2026-11-27", "status": "HALF_DAY", "holidayName": "Day After Thanksgiving", "sessions": []any{
				map[string]any{"type": "REGULAR", "open": "2026-11-27T23:30:00+09:00", "close": "2026-11-28T03:00:00+09:00"},
			}},
			map[string]any{"date": "2026-12-25", "status": "CLOSED", "holidayName": "Christmas", "sessions": []any{}},
		}}, nil
	}
	days, err := c.GetCalendar()
	if err != nil {
		t.Fatal(err)
	}
	if !days[0].Open || days[0].Timezone != "America/New_York" || days[0].Regular.Start != "09:30" || days[0].Regular.End != "16:00" {
		t.Fatalf("day0 = %+v regular=%+v", days[0], days[0].Regular)
	}
	if !days[1].Open || days[1].Regular.End != "13:00" || days[1].Holiday != "Day After Thanksgiving" {
		t.Fatalf("half day = %+v regular=%+v", days[1], days[1].Regular)
	}
	if days[2].Open || days[2].Regular != nil {
		t.Fatalf("closed day = %+v", days[2])
	}
}

func TestNextAccountPortfolioValue(t *testing.T) {
	c := NewNextClient("k", "s")
	c.call = func(method, path string, account bool, jsonBody map[string]any) (map[string]any, error) {
		if path == "/v1/account/holdings" {
			return map[string]any{"currency": "USD", "holdings": []any{map[string]any{
				"symbol": "AAPL", "name": "Apple Inc.", "quantity": "2", "sellableQuantity": "2", "averageBuyPrice": "300.00",
				"currentPrice": "310.00", "purchaseAmount": "600.00", "evaluationAmount": "620.00",
				"evaluationPnl": "20.00", "evaluationPnlRate": "3.3333",
			}}}, nil
		}
		return map[string]any{"accountId": "acc_main", "currency": "USD", "cashAmount": "1000.50", "requestedAt": "2026-09-10T23:10:00+09:00"}, nil
	}
	account, err := c.GetAccount()
	if err != nil {
		t.Fatal(err)
	}
	if account.Cash.String() != "1000.5" || account.PortfolioValue.String() != "1620.5" {
		t.Fatalf("account = %+v", account)
	}
	holdings, _ := c.GetHoldings()
	if holdings[0].AvgEntryPrice.String() != "300" || holdings[0].MarketValue.String() != "620" || holdings[0].UnrealizedPnlRate.String() != "0.033333" {
		t.Fatalf("holding = %+v", holdings[0])
	}
}

func TestNextOrderDetailAndCreateBody(t *testing.T) {
	c := NewNextClient("k", "s")
	var sent map[string]any
	c.call = func(method, path string, account bool, jsonBody map[string]any) (map[string]any, error) {
		if method == "POST" {
			sent = jsonBody
			return map[string]any{"orderId": "ord_2", "market": "US", "status": "SUBMITTED", "requestedAt": "2026-09-10T23:10:00+09:00"}, nil
		}
		return map[string]any{
			"orderId": "ord_1", "requestId": "s-1", "market": "US", "symbol": "AAPL", "side": "BUY", "orderType": "LIMIT",
			"timeInForce": "DAY", "status": "PENDING_CANCEL", "quantity": "10", "filledQuantity": "4", "limitPrice": "200",
			"requestedAt": "2026-09-10T23:10:00+09:00", "updatedAt": "2026-09-10T23:12:00+09:00",
		}, nil
	}
	order, err := c.GetOrder("ord_1")
	if err != nil {
		t.Fatal(err)
	}
	if order.ClientOrderID != "s-1" || order.Status != PendingCancel || !order.Status.IsOpen() || order.OrderType != Limit {
		t.Fatalf("order = %+v", order)
	}
	if got := order.SubmittedAt.UTC().Format(time.RFC3339); got != "2026-09-10T14:10:00Z" {
		t.Fatalf("submittedAt = %s", got)
	}
	limit := decimal.NewFromInt(200)
	if _, err := c.CreateOrder(CreateOrderRequest{Symbol: "AAPL", Side: Buy, OrderType: Limit, Quantity: decimal.NewFromInt(1), LimitPrice: &limit, ClientOrderID: "s-2"}); err != nil {
		t.Fatal(err)
	}
	if sent["market"] != "US" || sent["clientOrderId"] != "s-2" || sent["limitPrice"] != "200" || sent["orderType"] != "LIMIT" {
		t.Fatalf("body = %+v", sent)
	}
}

func TestKisQuotePercentToRate(t *testing.T) {
	c := NewKisClient("k", "s", "50199202")
	c.call = func(method, path, trID string, query, jsonBody map[string]string) (map[string]any, error) {
		return map[string]any{"output": map[string]any{
			"stck_prpr": "239500", "acml_vol": "27393580", "prdy_vrss": "-23000", "prdy_ctrt": "-8.76",
		}}, nil
	}
	quotes, err := c.GetQuotes([]string{"005930"})
	if err != nil {
		t.Fatal(err)
	}
	if quotes[0].Price.String() != "239500" {
		t.Fatalf("price = %s", quotes[0].Price)
	}
	if quotes[0].ChangeRate.String() != "-0.0876" {
		t.Fatalf("changeRate = %s", quotes[0].ChangeRate)
	}
}

func TestKisTrackedOrderFillByHoldings(t *testing.T) {
	c := NewKisClient("k", "s", "50199202")
	holdingQty := decimal.NewFromInt(2)
	c.call = func(method, path, trID string, query, jsonBody map[string]string) (map[string]any, error) {
		if trID == "VTTC8434R" { // 잔고
			return map[string]any{
				"output1": []any{map[string]any{"pdno": "005930", "hldg_qty": holdingQty.String(), "pchs_avg_pric": "0"}},
				"output2": []any{map[string]any{"dnca_tot_amt": "0", "tot_evlu_amt": "0"}},
			}, nil
		}
		return map[string]any{"output": map[string]any{"ODNO": "0000035787"}}, nil
	}

	limitPrice := decimal.NewFromInt(185400)
	order, err := c.CreateOrder(CreateOrderRequest{
		Symbol: "005930", Side: Buy, OrderType: Limit,
		Quantity: decimal.NewFromInt(1), LimitPrice: &limitPrice,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := c.GetOrder(order.OrderID)
	if got.Status != Submitted {
		t.Fatalf("status = %s", got.Status)
	}
	holdingQty = decimal.NewFromInt(3) // 체결로 보유 증가
	got, _ = c.GetOrder(order.OrderID)
	if got.Status != Filled {
		t.Fatalf("status = %s, want FILLED", got.Status)
	}
}

func TestKiwoomSignAndPadding(t *testing.T) {
	c := NewKiwoomClient("k", "s")
	c.call = func(path, apiID string, jsonBody map[string]string) (map[string]any, error) {
		return map[string]any{
			"cur_prc": "-239500", "trde_qty": "27393580", "pred_pre": "-23000", "flu_rt": "-8.76",
		}, nil
	}
	quotes, err := c.GetQuotes([]string{"005930"})
	if err != nil {
		t.Fatal(err)
	}
	if quotes[0].Price.String() != "239500" { // 부호 제거
		t.Fatalf("price = %s", quotes[0].Price)
	}
	if quotes[0].Change.String() != "-23000" { // 대비는 부호 유지
		t.Fatalf("change = %s", quotes[0].Change)
	}
}

func TestKiwoomHoldingsStripAPrefix(t *testing.T) {
	c := NewKiwoomClient("k", "s")
	c.call = func(path, apiID string, jsonBody map[string]string) (map[string]any, error) {
		return map[string]any{"acnt_evlt_remn_indv_tot": []any{map[string]any{
			"stk_cd": "A005930", "rmnd_qty": "000000000001", "pur_pric": "000000239500",
			"cur_prc": "-240000", "evlt_amt": "000000240000", "evltv_prft": "500", "prft_rt": "0.21",
		}}}, nil
	}
	holdings, err := c.GetHoldings()
	if err != nil {
		t.Fatal(err)
	}
	if holdings[0].Symbol != "005930" {
		t.Fatalf("symbol = %s", holdings[0].Symbol)
	}
	if holdings[0].CurrentPrice.String() != "240000" {
		t.Fatalf("currentPrice = %s", holdings[0].CurrentPrice)
	}
	if holdings[0].AvgEntryPrice.String() != "239500" { // zero-padded
		t.Fatalf("avgEntryPrice = %s", holdings[0].AvgEntryPrice)
	}
}

func TestKrxTickRound(t *testing.T) {
	cases := map[string]string{
		"246258.99": "246000", // 20만~50만: 500원
		"197650":    "197600", // 5만~20만: 100원
		"1999":      "1999",   // ~2천: 1원
	}
	for input, want := range cases {
		price, _ := decimal.NewFromString(input)
		if got := KrxTickRound(price).String(); got != want {
			t.Fatalf("KrxTickRound(%s) = %s, want %s", input, got, want)
		}
	}
}

// ------------------------------------------------------------- 0.6.0: 환경 · 심볼 접두

func TestNextEnvironmentMustMatchKeyPrefix(t *testing.T) {
	if err := NewNextClient("pk_test_x", "s").SetEnvironment(Live); err == nil {
		t.Fatal("pk_test_ 키로 LIVE 는 에러")
	}
	if err := NewNextClient("pk_live_x", "s").SetEnvironment(Paper); err == nil {
		t.Fatal("pk_live_ 키로 PAPER 는 에러")
	}
	c := NewNextClient("pk_live_x", "s")
	if err := c.SetEnvironment(Live); err != nil || c.Environment() != Live {
		t.Fatalf("LIVE 설정 실패: %v", err)
	}
	if NewNextClient("k", "s").Environment() != Paper {
		t.Fatal("기본은 PAPER")
	}
}

func TestNextMarketPrefixedSymbol(t *testing.T) {
	c := NewNextClient("k", "s")
	seen := ""
	c.call = func(method, path string, account bool, jsonBody map[string]any) (map[string]any, error) {
		seen = path
		return map[string]any{"quotes": []any{map[string]any{"symbol": "AAPL", "outcome": "OK", "price": "1", "requestedAt": "2026-09-10T23:10:00+09:00"}}}, nil
	}
	quotes, err := c.GetQuotes([]string{"US:AAPL"})
	if err != nil || quotes[0].Symbol != "US:AAPL" || seen != "/v1/market/quotes?symbols=AAPL" {
		t.Fatalf("quotes=%+v path=%s err=%v", quotes, seen, err)
	}
	if _, err := c.GetQuotes([]string{"KRX:005930"}); err == nil {
		t.Fatal("미지원 시장은 에러")
	}
}

func TestKisKiwoomEnvironmentHostAndTR(t *testing.T) {
	paper := NewKisClient("k", "s", "50199202")
	live := NewKisClient("k", "s", "50199202").SetEnvironment(Live)
	if paper.BaseURL() != KisPaperURL || paper.tr("TTC0802U") != "VTTC0802U" {
		t.Fatalf("paper = %s %s", paper.BaseURL(), paper.tr("TTC0802U"))
	}
	if live.BaseURL() != KisLiveURL || live.tr("TTC0802U") != "TTTC0802U" || live.Environment() != Live {
		t.Fatalf("live = %s %s", live.BaseURL(), live.tr("TTC0802U"))
	}
	if custom := NewKisClient("k", "s", "c").SetBaseURL("http://custom").SetEnvironment(Live); custom.BaseURL() != "http://custom" {
		t.Fatal("직접 지정한 호스트는 유지")
	}
	if NewKiwoomClient("k", "s").SetEnvironment(Live).BaseURL() != KiwoomLiveURL || NewKiwoomClient("k", "s").BaseURL() != KiwoomPaperURL {
		t.Fatal("kiwoom 호스트")
	}
}
