# Hermetix Go

증권사 모의투자 통합 트레이딩 프레임워크 — Go 구현 (Go 1.21+).

의존성은 `shopspring/decimal` 하나입니다. **금액에 float 를 절대 섞지 마세요.**

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
hermetix.NewKiwoomClient(appkey, secretkey)      // 키움 모의 (KRX, 일봉만)
```

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
