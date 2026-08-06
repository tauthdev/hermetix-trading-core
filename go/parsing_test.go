package hermetix

// 어댑터 정규화 검증 - 실측 골든 픽스처 재생 (Kotlin/Python/JS 와 동일 정답지).

import (
	"testing"

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

func TestNextQuoteParsing(t *testing.T) {
	c := NewNextClient("k", "s")
	c.call = func(method, path string, account bool, jsonBody map[string]any) (map[string]any, error) {
		return map[string]any{"quotes": []any{map[string]any{
			"symbol": "AAPL", "price": "308.91", "bidPrice": nil, "askPrice": nil,
			"volume": float64(132756799), "change": "-24.52", "changeRate": "-0.073539",
			"timestamp": "2026-07-31T04:00:00Z",
		}}}, nil
	}
	quotes, err := c.GetQuotes([]string{"AAPL"})
	if err != nil {
		t.Fatal(err)
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
