package hermetix

// 골든 픽스처의 stream 섹션 — 네 언어가 같은 실측 프레임을 같은 TradeTick 으로 파싱하는지 (Kotlin StreamFixtureTest 와 동일 정답지).

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

type streamFixture struct {
	Channel  string   `json:"channel"`
	Frames   []string `json:"frames"`
	Expected []struct {
		Symbol           string `json:"symbol"`
		Price            string `json:"price"`
		Quantity         string `json:"quantity"`
		Time             string `json:"time"`
		AskPrice         string `json:"askPrice"`
		BidPrice         string `json:"bidPrice"`
		CumulativeVolume int64  `json:"cumulativeVolume"`
		Change           string `json:"change"`
		ChangeRate       string `json:"changeRate"`
	} `json:"expected"`
}

func loadStreamFixture(t *testing.T, broker string) streamFixture {
	t.Helper()
	raw, err := os.ReadFile("../conformance/fixtures/" + broker + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Stream *streamFixture `json:"stream"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if file.Stream == nil {
		t.Fatalf("%s 픽스처에 stream 섹션이 없다", broker)
	}
	if file.Stream.Channel != string(StreamTrades) {
		t.Fatalf("channel = %s", file.Stream.Channel)
	}
	return *file.Stream
}

func assertDecimalEqual(t *testing.T, label string, actual *decimal.Decimal, expected string) {
	t.Helper()
	if actual == nil {
		t.Fatalf("%s: nil (expected %s)", label, expected)
	}
	want, err := decimal.NewFromString(expected)
	if err != nil {
		t.Fatal(err)
	}
	if !actual.Equal(want) {
		t.Fatalf("%s = %s, expected %s", label, actual, want)
	}
}

func assertStreamTicks(t *testing.T, ticks []TradeTick, fx streamFixture) {
	t.Helper()
	if len(ticks) != len(fx.Expected) {
		t.Fatalf("ticks = %d, expected %d", len(ticks), len(fx.Expected))
	}
	for i, e := range fx.Expected {
		tick := ticks[i]
		if tick.Symbol != e.Symbol {
			t.Fatalf("[%d] symbol = %s, expected %s", i, tick.Symbol, e.Symbol)
		}
		assertDecimalEqual(t, "price", &tick.Price, e.Price)
		assertDecimalEqual(t, "quantity", &tick.Quantity, e.Quantity)
		assertDecimalEqual(t, "askPrice", tick.AskPrice, e.AskPrice)
		assertDecimalEqual(t, "bidPrice", tick.BidPrice, e.BidPrice)
		if tick.CumulativeVolume == nil || *tick.CumulativeVolume != e.CumulativeVolume {
			t.Fatalf("[%d] cumulativeVolume = %v, expected %d", i, tick.CumulativeVolume, e.CumulativeVolume)
		}
		assertDecimalEqual(t, "change", tick.Change, e.Change)
		assertDecimalEqual(t, "changeRate", tick.ChangeRate, e.ChangeRate)
		if got := tick.Timestamp.In(kst).Format("15:04:05"); got != e.Time {
			t.Fatalf("[%d] time = %s, expected %s", i, got, e.Time)
		}
	}
}

var fixtureToday = time.Date(2026, 9, 14, 0, 0, 0, 0, kst)

func TestKisStreamFixture(t *testing.T) {
	fx := loadStreamFixture(t, "kis")
	ticks := make([]TradeTick, 0)
	for _, frame := range fx.Frames {
		ticks = append(ticks, ParseKisFrame(frame, fixtureToday)...)
	}
	assertStreamTicks(t, ticks, fx)
}

func TestKiwoomStreamFixture(t *testing.T) {
	fx := loadStreamFixture(t, "kiwoom")
	ticks := make([]TradeTick, 0)
	for _, frame := range fx.Frames {
		ticks = append(ticks, ParseKiwoomReal([]byte(frame), fixtureToday)...)
	}
	assertStreamTicks(t, ticks, fx)
}

func TestKisFrameParsingEdgeCases(t *testing.T) {
	record := "005930^093012^71500^5^300^-0.42^71480^71800^71900^71300^71500^71400^15^1234567^88000000000^1200^1300^100^105.2^600000"
	one := ParseKisFrame("0|H0STCNT0|001|"+record, fixtureToday)
	if len(one) != 1 {
		t.Fatalf("records = %d", len(one))
	}
	assertDecimalEqual(t, "change", one[0].Change, "-300") // 부호코드 5(하락) + 무부호 값 → 음수
	assertDecimalEqual(t, "changeRate", one[0].ChangeRate, "-0.0042")
	if two := ParseKisFrame("0|H0STCNT0|002|"+record+"^"+record, fixtureToday); len(two) != 2 {
		t.Fatalf("2 records = %d", len(two))
	}
	if other := ParseKisFrame("0|H0STASP0|001|"+record, fixtureToday); len(other) != 0 {
		t.Fatal("다른 TR 은 무시")
	}
	if short := ParseKisFrame("0|H0STCNT0|001|005930^093012", fixtureToday); len(short) != 0 {
		t.Fatal("필드 부족은 무시")
	}
}

func TestKiwoomRealIgnoresOtherTypes(t *testing.T) {
	orderbook := `{"trnm":"REAL","data":[{"type":"0D","name":"주식호가잔량","item":"005930","values":{"21":"093012"}}]}`
	if ticks := ParseKiwoomReal([]byte(orderbook), fixtureToday); len(ticks) != 0 {
		t.Fatal("0B 외 타입은 무시")
	}
	real := `{"trnm":"REAL","data":[{"type":"0B","name":"주식체결","item":"A005930","values":{"20":"093012","10":"-71500","11":"-300","12":"-0.42","27":"+71500","28":"+71400","15":"-15","13":"1234567"}}]}`
	ticks := ParseKiwoomReal([]byte(real), fixtureToday)
	if len(ticks) != 1 || ticks[0].Symbol != "005930" {
		t.Fatalf("ticks = %+v", ticks)
	}
	assertDecimalEqual(t, "price", &ticks[0].Price, "71500")
	assertDecimalEqual(t, "quantity", &ticks[0].Quantity, "15")
	assertDecimalEqual(t, "bidPrice", ticks[0].BidPrice, "71400")
}
