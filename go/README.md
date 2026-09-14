# Hermetix Go

증권사 모의투자 통합 트레이딩 프레임워크 — Go 구현 (Go 1.21+).

의존성은 `shopspring/decimal` 과 실시간 스트림용 `coder/websocket` 둘입니다. **금액에 float 를 절대 섞지 마세요.**

## 빠른 시작

```go
import (
    "github.com/shopspring/decimal"
    hermetix "github.com/tauthdev/hermetix-trading-core/go"
)

type MyStrategy struct{}

func (s MyStrategy) Spec() hermetix.StrategySpec {
    return hermetix.StrategySpec{Name: "my-first", Symbols: []string{"AAPL"}}
}

func (s MyStrategy) Decide(ctx *hermetix.StrategyContext) ([]hermetix.Signal, error) {
    q, ok := ctx.Quote("AAPL")
    if ok && !ctx.HasPosition("AAPL") && !ctx.HasOpenOrder("AAPL") {
        tp := q.Price.Mul(decimal.NewFromFloat(1.04))
        sl := q.Price.Mul(decimal.NewFromFloat(0.98))
        return []hermetix.Signal{hermetix.BuySignal{
            Symbol: "AAPL", Quantity: decimal.NewFromInt(1),
            TakeProfitPrice: &tp, StopLossPrice: &sl,  // 익절/손절은 엔진이 자동 실행
        }}, nil
    }
    return nil, nil
}

func main() {
    broker := hermetix.NewNextClient("pk_test_...", "sk_test_...")
    hermetix.NewStrategyEngine(broker, []hermetix.Strategy{MyStrategy{}}).Run()
}
```

브로커 전환은 생성자 교체 한 줄:

```go
hermetix.NewKisClient(appkey, appsecret, cano)   // 한국투자 모의 (KRX, 일봉만)
hermetix.NewNhClient(appKey, appSecret, "")     // NH투자증권 NH PLUG (⚠️ 미검증, 문서 기반) — 계좌 비우면 자동 선택
hermetix.NewDbClient(appKey, appSecret)         // DB증권 (⚠️ 미검증, 문서 기반)
hermetix.NewLsClient(appKey, appSecret)         // LS증권 (⚠️ 미검증, 문서 기반)
hermetix.NewTossClient(clientID, secret, "")    // 토스증권 (⚠️ 실전 전용, 미검증) — LiveTradingEnabled 필수, 계좌 비우면 첫 위탁계좌
hermetix.NewKbClient(appKey, appSecret)         // KB증권 오픈베타 (⚠️ 실전 전용, 미검증) — LiveTradingEnabled 필수
hermetix.NewKiwoomClient(appkey, secretkey)      // 키움 모의 (KRX, 일봉만)
```

## 실전투자로 전환

모의에서 검증한 뒤 실제 계좌로 옮길 때는 환경과 명시 동의를 함께 지정합니다. 하나라도 빠지면 엔진이 전략을 스케줄하지 않습니다.

```go
broker := hermetix.NewKisClient(appkey, appsecret, cano).SetEnvironment(hermetix.Live)  // 호스트·TR ID 자동 전환
maxOrder := decimal.NewFromInt(1_000_000)
maxDaily := decimal.NewFromInt(5_000_000)
hermetix.NewStrategyEngineWithOptions(broker, []hermetix.Strategy{MyStrategy{}}, hermetix.EngineOptions{
    LiveTradingEnabled: true,       // 실전 명시 동의
    MaxOrderValue:      &maxOrder,  // 주문 1건 상한 (브로커 통화)
    MaxDailyOrderValue: &maxDaily,  // 하루(UTC) 누적 상한 — 매수·매도 합산
}).Run()
```

- 넥스트증권은 키 프리픽스가 환경을 결정합니다 (`pk_test_`=모의, `pk_live_`=실전). `SetEnvironment` 가 어긋나면 error 를 돌려줍니다
- 심볼에 시장 접두를 붙일 수 있습니다 (`KRX:005930`, `US:AAPL`). 접두 없는 심볼은 브로커 기본 시장으로 해석됩니다
- 키는 항상 당신의 기기에서만 쓰입니다. Hermetix 는 어떤 서버로도 키를 보내지 않습니다

## 실시간 체결가 트리거 (0.8.0)

`StrategySpec.Trigger = hermetix.TriggerOnTrade` 로 선언하면 브로커 체결가 웹소켓 틱마다 `Decide` 가 호출됩니다. 몰려온 틱은 하나로 합치고 `MinTickInterval`(기본 1s)보다 촘촘히는 부르지 않으며, 스트림이 끊기면 `PollInterval` 폴링이 안전망으로 계속 돕니다. 전략 코드는 바뀌지 않습니다.

```go
hermetix.StrategySpec{
    Name: "scalp", Symbols: []string{"005930"},
    Trigger: hermetix.TriggerOnTrade, MinTickInterval: 2 * time.Second,
}
```

- 지원 브로커: `kis`(H0STCNT0), `kiwoom`(0B) — 2026-09 모의 웹소켓 장중 실측. 넥스트증권은 공개 스펙에 웹소켓이 없어 폴링만
- 스트림 틱이 전략의 모든 심볼을 덮으면 `ctx.Quote()` 는 REST 대신 마지막 체결 틱으로 채워집니다. 캔들·계좌·미체결은 여전히 REST 라 틱마다 `1 + 심볼 수 + 3` 호출이 나갑니다 — 모의 서버 레이트리밋(kis 2건/s)을 생각해 `MinTickInterval` 을 잡으세요
- 스트림을 선언하지 않은 브로커에서 `TriggerOnTrade` 를 쓰면 경고 로그 후 폴링으로 동작합니다
- 연결 계층만 쓸 때: `client.OpenStream()` → `Connect()` → `SubscribeTrades(symbols, func(tick hermetix.TradeTick) {...})`. 재접속·구독 복원은 스트림이 알아서 합니다

## 공식 전략 예제 (examples/)

Kotlin 전략 레포 3종과 동일 로직 (`examples` 패키지, 임포트 가능):

```bash
HERMETIX_BROKER=next NEXT_CLIENT_ID=... go run ./cmd/example larry
HERMETIX_BROKER=kis  KIS_APPKEY=...     go run ./cmd/example grid   # KRX 호환
```

## 테스트

```bash
cd go
go test ./...                # 오프라인 (골든 픽스처 재생)
go run ./cmd/smoke next      # 실서버 (환경변수로 키 주입)
```

동작 상세는 [코어 문서](../docs/architecture.md)와 동일합니다.
