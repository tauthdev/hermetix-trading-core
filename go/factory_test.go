package hermetix

// 브로커 팩토리 — docs/broker-factory.md 규약대로 자격 증명이 기존 생성자 인자·setter 로 매핑되는지. 네트워크 없음.

import (
	"strings"
	"testing"
	"time"
)

func TestFactoryBrokersOrder(t *testing.T) {
	want := []string{"next", "kis", "kiwoom", "nh", "ls", "db", "toss", "kb"}
	if len(Brokers) != len(want) {
		t.Fatalf("Brokers=%v", Brokers)
	}
	for i := range want {
		if Brokers[i] != want[i] {
			t.Fatalf("Brokers[%d]=%q want %q", i, Brokers[i], want[i])
		}
	}
}

func TestFactoryNext(t *testing.T) {
	c, err := Next(Credentials{APIKey: "pk_test_x", APISecret: "sk", Account: "acc_1", Extra: map[string]string{"base_url": "http://h"}})
	if err != nil {
		t.Fatal(err)
	}
	if c.clientID != "pk_test_x" || c.clientSecret != "sk" || c.accountID != "acc_1" || c.baseURL != "http://h" || c.Environment() != Paper {
		t.Fatalf("next mapping: %+v", c)
	}
	// Account 생략 → acc_main, 환경은 키 프리픽스와 맞아야 한다
	d, _ := Next(Credentials{APIKey: "pk_test_x", APISecret: "sk"})
	if d.accountID != "acc_main" {
		t.Fatalf("default account=%q", d.accountID)
	}
	if _, err := Next(Credentials{APIKey: "pk_test_x", APISecret: "sk", Environment: Live}); err == nil {
		t.Fatal("pk_test_ 키에 LIVE 는 에러여야 한다")
	}
	if _, err := Next(Credentials{APIKey: "pk_test_x", APISecret: "sk", Extra: map[string]string{"ws_url": "ws://x"}}); err == nil || !strings.Contains(err.Error(), "ws_url") {
		t.Fatalf("next 는 ws_url 을 지원하지 않아야 한다: %v", err)
	}
}

func TestFactoryKis(t *testing.T) {
	c, err := Kis(Credentials{APIKey: "ak", APISecret: "as", Account: "12345678", Environment: Live,
		Extra: map[string]string{"acnt_prdt_cd": "22", "hts_id": "hts", "throttle_seconds": "0.25", "ws_url": "ws://custom"}})
	if err != nil {
		t.Fatal(err)
	}
	if c.appkey != "ak" || c.appsecret != "as" || c.cano != "12345678" || c.acntPrdtCd != "22" || c.htsID != "hts" {
		t.Fatalf("kis mapping: %+v", c)
	}
	if c.Environment() != Live || c.BaseURL() != KisLiveURL || c.wsURL != "ws://custom" {
		t.Fatalf("kis env/url: env=%s base=%s ws=%s", c.Environment(), c.BaseURL(), c.wsURL)
	}
	if c.limiter.throttle.interval != 250*time.Millisecond {
		t.Fatalf("throttle=%v", c.limiter.throttle.interval)
	}
	if _, err := Kis(Credentials{APIKey: "ak", APISecret: "as"}); err == nil {
		t.Fatal("kis 는 Account(cano) 필수")
	}
	if _, err := Kis(Credentials{APIKey: "ak", APISecret: "as", Account: "1", Extra: map[string]string{"custtype": "P"}}); err == nil {
		t.Fatal("Go kis 에 없는 custtype 은 에러")
	}
}

func TestFactoryKiwoom(t *testing.T) {
	c, err := Kiwoom(Credentials{APIKey: "k", APISecret: "s", Account: "ignored", Environment: Live})
	if err != nil {
		t.Fatal(err)
	}
	if c.appkey != "k" || c.secretkey != "s" || c.Environment() != Live || c.baseURL != KiwoomLiveURL {
		t.Fatalf("kiwoom mapping: %+v", c)
	}
	d, _ := Kiwoom(Credentials{APIKey: "k", APISecret: "s"})
	if d.Environment() != Paper {
		t.Fatalf("kiwoom default env=%s", d.Environment())
	}
}

func TestFactoryNh(t *testing.T) {
	c, err := Nh(Credentials{APIKey: "k", APISecret: "s", Account: "acct",
		Extra: map[string]string{"auth_url": "http://auth", "market_cd": "NXT", "order_market_cd": "KRX", "throttle_seconds": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if c.appKey != "k" || c.appSecret != "s" || c.accountNo != "acct" || c.authURL != "http://auth" || c.marketCd != "NXT" || c.orderMarketCd != "KRX" {
		t.Fatalf("nh mapping: %+v", c)
	}
	if c.limiter.throttle.interval != time.Second {
		t.Fatalf("throttle=%v", c.limiter.throttle.interval)
	}
}

func TestFactoryLs(t *testing.T) {
	c, err := Ls(Credentials{APIKey: "k", APISecret: "s", Extra: map[string]string{"mac_address": "aa:bb", "exch_gubun": "K", "chart_throttle_seconds": "2"}})
	if err != nil {
		t.Fatal(err)
	}
	if c.appKey != "k" || c.appSecret != "s" || c.macAddress != "aa:bb" || c.exchGubun != "K" || c.chartLimiter.throttle.interval != 2*time.Second {
		t.Fatalf("ls mapping: %+v", c)
	}
}

func TestFactoryDb(t *testing.T) {
	c, err := Db(Credentials{APIKey: "k", APISecret: "s", Extra: map[string]string{"mac_address": "m", "market_div_code": "Q"}})
	if err != nil {
		t.Fatal(err)
	}
	if c.appKey != "k" || c.appSecret != "s" || c.macAddress != "m" || c.marketDivCode != "Q" || c.baseURL != DbBaseURL {
		t.Fatalf("db mapping: %+v", c)
	}
}

func TestFactoryToss(t *testing.T) {
	c, err := Toss(Credentials{APIKey: "id", APISecret: "sec", Account: "7"})
	if err != nil {
		t.Fatal(err)
	}
	if c.clientID != "id" || c.clientSecret != "sec" || c.accountSeq != "7" || c.Environment() != Live || c.baseURL != TossBaseURL {
		t.Fatalf("toss mapping: %+v", c)
	}
}

func TestFactoryKb(t *testing.T) {
	c, err := Kb(Credentials{APIKey: "k", APISecret: "s", Extra: map[string]string{"excg_clsf": "2", "sor_order_ccd": "N", "chart_market_clsf": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if c.appKey != "k" || c.appSecret != "s" || c.excgClsf != "2" || c.sorOrderCcd != "N" || c.chartMarketClsf != "1" || c.Environment() != Live {
		t.Fatalf("kb mapping: %+v", c)
	}
	if _, err := Kb(Credentials{APIKey: "k", APISecret: "s", Extra: map[string]string{"ws_url": "ws://x"}}); err == nil {
		t.Fatal("kb 는 웹소켓이 없어 ws_url 을 거부해야 한다")
	}
}

func TestFactoryClientAndErrors(t *testing.T) {
	for _, id := range Brokers {
		cred := Credentials{APIKey: "pk_test_k", APISecret: "s", Account: "a"}
		c, err := Client(id, cred)
		if err != nil {
			t.Fatalf("Client(%q): %v", id, err)
		}
		if c.Capabilities().BrokerID != id {
			t.Fatalf("Client(%q) → BrokerID %q", id, c.Capabilities().BrokerID)
		}
	}
	if _, err := Client("binance", Credentials{APIKey: "k", APISecret: "s"}); err == nil || !strings.Contains(err.Error(), "binance") {
		t.Fatalf("unknown id: %v", err)
	}
	if _, err := Client("kis", Credentials{APISecret: "s", Account: "a"}); err == nil {
		t.Fatal("APIKey 비면 에러")
	}
	if _, err := Client("kiwoom", Credentials{APIKey: "k", APISecret: "s", Environment: "SANDBOX"}); err == nil {
		t.Fatal("Environment 는 PAPER/LIVE 만")
	}
	if _, err := Client("kiwoom", Credentials{APIKey: "k", APISecret: "s", Extra: map[string]string{"throttle_seconds": "fast"}}); err == nil {
		t.Fatal("throttle_seconds 는 숫자여야 한다")
	}
	if _, err := Client("kiwoom", Credentials{APIKey: "k", APISecret: "s", Extra: map[string]string{"typo_key": "1"}}); err == nil || !strings.Contains(err.Error(), "typo_key") {
		t.Fatalf("모르는 extra 키는 에러: %v", err)
	}
}
