package hermetix

// 브로커 팩토리 — docs/broker-factory.md 규약. ccxt 의 `new ccxt.binance({...})` 처럼 브로커 ID 한 토큰만 바꾸면
// 증권사가 바뀐다. 자격 증명을 기존 생성자 인자·setter 로 매핑만 하고, 인증·환경 결정·쓰로틀 기본값은 기존 로직 그대로다.

import (
	"fmt"
	"sort"
	"strconv"
	"time"
)

// Credentials - 모든 브로커가 같은 모양으로 받는 자격 증명 (docs/broker-factory.md "자격 증명").
//   - APIKey/APISecret: 필수
//   - Account: next account_id(빈 값이면 acc_main) · kis cano(필수) · nh account_no · toss account_seq · kiwoom/db/ls/kb 는 무시
//   - Environment: 빈 값이면 브로커 기본값 (next/kis/kiwoom/nh/ls/db = Paper, toss/kb = Live)
//   - Extra: 브로커별 선택 항목. 키는 규약 표의 snake_case. 모르는 키·그 브로커가 지원하지 않는 키는 에러
type Credentials struct {
	APIKey      string
	APISecret   string
	Account     string
	Environment TradingEnvironment
	Extra       map[string]string
}

// Brokers - 팩토리가 아는 브로커 ID (BrokerCapabilities.BrokerID 와 같다). 순서는 규약 그대로.
var Brokers = []string{"next", "kis", "kiwoom", "nh", "ls", "db", "toss", "kb"}

// Client - 브로커 ID 로 생성. 모르는 ID 는 error.
func Client(id string, c Credentials) (BrokerClient, error) {
	switch id {
	case "next":
		return Next(c)
	case "kis":
		return Kis(c)
	case "kiwoom":
		return Kiwoom(c)
	case "nh":
		return Nh(c)
	case "ls":
		return Ls(c)
	case "db":
		return Db(c)
	case "toss":
		return Toss(c)
	case "kb":
		return Kb(c)
	}
	return nil, fmt.Errorf("hermetix: 모르는 브로커 ID %q (가능: %v)", id, Brokers)
}

// Next - 넥스트증권. extra: base_url. environment 는 키 프리픽스(pk_test_/pk_live_)와 맞아야 한다.
func Next(c Credentials) (*NextClient, error) {
	if err := c.require("next", false); err != nil {
		return nil, err
	}
	ex, err := c.extra("next", "base_url")
	if err != nil {
		return nil, err
	}
	client := NewNextClient(c.APIKey, c.APISecret)
	if c.Account != "" {
		client.accountID = c.Account
	}
	if c.Environment != "" {
		if err := client.SetEnvironment(c.Environment); err != nil {
			return nil, err
		}
	}
	if v, ok := ex["base_url"]; ok {
		client.SetBaseURL(v)
	}
	return client, nil
}

// Kis - 한국투자증권. Account = cano(필수). extra: base_url, ws_url, throttle_seconds, acnt_prdt_cd, hts_id.
func Kis(c Credentials) (*KisClient, error) {
	if err := c.require("kis", true); err != nil {
		return nil, err
	}
	ex, err := c.extra("kis", "base_url", "ws_url", "throttle_seconds", "acnt_prdt_cd", "hts_id")
	if err != nil {
		return nil, err
	}
	client := NewKisClient(c.APIKey, c.APISecret, c.Account)
	if c.Environment != "" {
		client.SetEnvironment(c.Environment)
	}
	if v, ok := ex["base_url"]; ok {
		client.SetBaseURL(v)
	}
	if v, ok := ex["ws_url"]; ok {
		client.SetWSURL(v)
	}
	if v, ok := ex["acnt_prdt_cd"]; ok {
		client.acntPrdtCd = v
	}
	if v, ok := ex["hts_id"]; ok {
		client.SetHTSID(v)
	}
	if d, ok, err := extraThrottle(ex); err != nil {
		return nil, err
	} else if ok {
		client.SetThrottle(d)
	}
	return client, nil
}

// Kiwoom - 키움증권. Account 는 쓰지 않는다. extra: base_url, ws_url, throttle_seconds.
func Kiwoom(c Credentials) (*KiwoomClient, error) {
	if err := c.require("kiwoom", false); err != nil {
		return nil, err
	}
	ex, err := c.extra("kiwoom", "base_url", "ws_url", "throttle_seconds")
	if err != nil {
		return nil, err
	}
	client := NewKiwoomClient(c.APIKey, c.APISecret)
	if c.Environment != "" {
		client.SetEnvironment(c.Environment)
	}
	if v, ok := ex["base_url"]; ok {
		client.SetBaseURL(v)
	}
	if v, ok := ex["ws_url"]; ok {
		client.SetWSURL(v)
	}
	if d, ok, err := extraThrottle(ex); err != nil {
		return nil, err
	} else if ok {
		client.SetThrottle(d)
	}
	return client, nil
}

// Nh - NH투자증권. Account = account_no(빈 값이면 첫 계좌 자동 선택). extra: base_url, ws_url, throttle_seconds, auth_url, market_cd, order_market_cd.
func Nh(c Credentials) (*NhClient, error) {
	if err := c.require("nh", false); err != nil {
		return nil, err
	}
	ex, err := c.extra("nh", "base_url", "ws_url", "throttle_seconds", "auth_url", "market_cd", "order_market_cd")
	if err != nil {
		return nil, err
	}
	client := NewNhClient(c.APIKey, c.APISecret, c.Account)
	if c.Environment != "" {
		client.SetEnvironment(c.Environment)
	}
	if v, ok := ex["base_url"]; ok {
		client.SetBaseURL(v)
	}
	if v, ok := ex["ws_url"]; ok {
		client.SetWSURL(v)
	}
	if v, ok := ex["auth_url"]; ok {
		client.SetAuthURL(v)
	}
	if v, ok := ex["market_cd"]; ok {
		client.marketCd = v
	}
	if v, ok := ex["order_market_cd"]; ok {
		client.orderMarketCd = v
	}
	if d, ok, err := extraThrottle(ex); err != nil {
		return nil, err
	} else if ok {
		client.SetThrottle(d)
	}
	return client, nil
}

// Ls - LS증권. Account 는 쓰지 않는다. extra: base_url, ws_url, throttle_seconds, chart_throttle_seconds, mac_address, exch_gubun.
func Ls(c Credentials) (*LsClient, error) {
	if err := c.require("ls", false); err != nil {
		return nil, err
	}
	ex, err := c.extra("ls", "base_url", "ws_url", "throttle_seconds", "chart_throttle_seconds", "mac_address", "exch_gubun")
	if err != nil {
		return nil, err
	}
	client := NewLsClient(c.APIKey, c.APISecret)
	if c.Environment != "" {
		client.SetEnvironment(c.Environment)
	}
	if v, ok := ex["base_url"]; ok {
		client.SetBaseURL(v)
	}
	if v, ok := ex["ws_url"]; ok {
		client.SetWSURL(v)
	}
	if v, ok := ex["mac_address"]; ok {
		client.SetMacAddress(v)
	}
	if v, ok := ex["exch_gubun"]; ok {
		client.SetExchGubun(v)
	}
	if d, ok, err := extraThrottle(ex); err != nil {
		return nil, err
	} else if ok {
		client.SetThrottle(d)
	}
	if v, ok := ex["chart_throttle_seconds"]; ok {
		d, err := seconds("chart_throttle_seconds", v)
		if err != nil {
			return nil, err
		}
		client.SetChartThrottle(d)
	}
	return client, nil
}

// Db - DB증권. Account 는 쓰지 않는다. extra: base_url, ws_url, throttle_seconds, mac_address, market_div_code.
func Db(c Credentials) (*DbClient, error) {
	if err := c.require("db", false); err != nil {
		return nil, err
	}
	ex, err := c.extra("db", "base_url", "ws_url", "throttle_seconds", "mac_address", "market_div_code")
	if err != nil {
		return nil, err
	}
	client := NewDbClient(c.APIKey, c.APISecret)
	if c.Environment != "" {
		client.SetEnvironment(c.Environment)
	}
	if v, ok := ex["base_url"]; ok {
		client.SetBaseURL(v)
	}
	if v, ok := ex["ws_url"]; ok {
		client.SetWSURL(v)
	}
	if v, ok := ex["mac_address"]; ok {
		client.SetMacAddress(v)
	}
	if v, ok := ex["market_div_code"]; ok {
		client.marketDivCode = v
	}
	if d, ok, err := extraThrottle(ex); err != nil {
		return nil, err
	} else if ok {
		client.SetThrottle(d)
	}
	return client, nil
}

// Toss - 토스증권. Account = account_seq. extra: base_url, ws_url, throttle_seconds.
func Toss(c Credentials) (*TossClient, error) {
	if err := c.require("toss", false); err != nil {
		return nil, err
	}
	ex, err := c.extra("toss", "base_url", "ws_url", "throttle_seconds")
	if err != nil {
		return nil, err
	}
	client := NewTossClient(c.APIKey, c.APISecret, c.Account)
	if c.Environment != "" {
		client.SetEnvironment(c.Environment)
	}
	if v, ok := ex["base_url"]; ok {
		client.SetBaseURL(v)
	}
	if v, ok := ex["ws_url"]; ok {
		client.SetWSURL(v)
	}
	if d, ok, err := extraThrottle(ex); err != nil {
		return nil, err
	} else if ok {
		client.SetThrottle(d)
	}
	return client, nil
}

// Kb - KB증권. Account 는 쓰지 않는다. 웹소켓이 없어 ws_url 은 받지 않는다. extra: base_url, throttle_seconds, excg_clsf, sor_order_ccd, chart_market_clsf.
func Kb(c Credentials) (*KbClient, error) {
	if err := c.require("kb", false); err != nil {
		return nil, err
	}
	ex, err := c.extra("kb", "base_url", "throttle_seconds", "excg_clsf", "sor_order_ccd", "chart_market_clsf")
	if err != nil {
		return nil, err
	}
	client := NewKbClient(c.APIKey, c.APISecret)
	if c.Environment != "" {
		client.SetEnvironment(c.Environment)
	}
	if v, ok := ex["base_url"]; ok {
		client.SetBaseURL(v)
	}
	if v, ok := ex["excg_clsf"]; ok {
		client.SetExcgClsf(v)
	}
	if v, ok := ex["sor_order_ccd"]; ok {
		client.SetSorOrderCcd(v)
	}
	if v, ok := ex["chart_market_clsf"]; ok {
		client.SetChartMarketClsf(v)
	}
	if d, ok, err := extraThrottle(ex); err != nil {
		return nil, err
	} else if ok {
		client.SetThrottle(d)
	}
	return client, nil
}

// require - 필수 값 검사. accountRequired 인 브로커(kis)는 Account 도 비면 에러.
func (c Credentials) require(broker string, accountRequired bool) error {
	if c.APIKey == "" || c.APISecret == "" {
		return fmt.Errorf("hermetix.%s: APIKey 와 APISecret 은 필수입니다", broker)
	}
	if accountRequired && c.Account == "" {
		return fmt.Errorf("hermetix.%s: Account(계좌)는 필수입니다", broker)
	}
	if c.Environment != "" && c.Environment != Paper && c.Environment != Live {
		return fmt.Errorf("hermetix.%s: Environment 는 PAPER 또는 LIVE 여야 합니다 (받은 값 %q)", broker, string(c.Environment))
	}
	return nil
}

// extra - 허용된 키만 통과. 모르는 키(오타 포함)는 조용히 무시하지 않고 에러로 알린다.
func (c Credentials) extra(broker string, allowed ...string) (map[string]string, error) {
	ok := map[string]bool{}
	for _, k := range allowed {
		ok[k] = true
	}
	var unknown []string
	for k := range c.Extra {
		if !ok[k] {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("hermetix.%s: 지원하지 않는 extra 키 %v (가능: %v)", broker, unknown, allowed)
	}
	return c.Extra, nil
}

// extraThrottle - extra["throttle_seconds"](초, 실수 문자열)를 time.Duration 으로.
func extraThrottle(ex map[string]string) (time.Duration, bool, error) {
	v, ok := ex["throttle_seconds"]
	if !ok {
		return 0, false, nil
	}
	d, err := seconds("throttle_seconds", v)
	return d, true, err
}

func seconds(key, v string) (time.Duration, error) {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("hermetix: extra %s 는 0 이상의 초 단위 실수여야 합니다 (받은 값 %q)", key, v)
	}
	return time.Duration(f * float64(time.Second)), nil
}
