# hermetix-trading-core

모의투자 **자동매매 전략 프레임워크**. 전략 한 번 작성하면 증권사는 설정으로 갈아끼웁니다.

- 지원 브로커: **넥스트증권 모의투자** (`broker: next`)
- 추가 예정: 한국투자증권(KIS) 모의투자, 키움증권 모의투자 — [아키텍처 문서](docs/architecture.md)의 BrokerClient 어댑터 구조 참조

전략 작성자는 `TradingStrategy` 인터페이스 하나만 구현하면 됩니다. 인증(OAuth 토큰 관리), 시세/계좌 조회, 주문 실행, 체결 추적, 익절/손절 관리, 장 운영시간 체크, 비상정지는 전부 코어가 처리합니다.

## 빠른 시작

### 1. 의존성 추가 (JitPack)

```kotlin
// settings.gradle.kts
repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

// build.gradle.kts
dependencies {
    implementation("com.github.tauthdev:hermetix-trading-core:0.3.0")
}
```

### 2. 설정

```yaml
# application.yml
hermetix:
  broker: next                   # 사용할 증권사 어댑터 (현재: next)
  next:
    client-id: pk_test_...       # 넥스트증권 모의투자 API 키
    client-secret: sk_test_...
    account-id: acc_main
  engine:
    max-consecutive-failures: 5  # 연속 실패 시 비상정지 임계치
```

> API 키는 커밋하지 마세요. `application-local.yml`(gitignore) 또는 환경변수를 사용하세요.

### 3. 전략 작성

```kotlin
@Component
class MyFirstStrategy : TradingStrategy {

    override val spec = StrategySpec(
        name = "my-first",
        symbols = listOf("AAPL"),
        candleInterval = CandleInterval.DAY_1,
        candleLimit = 20,
        pollInterval = Duration.ofSeconds(60),
    )

    override fun decide(context: StrategyContext): List<Signal> {
        val candles = context.candles("AAPL")
        if (candles.size < 20) return emptyList()

        val price = context.quote("AAPL")?.price ?: return emptyList()
        val ma20 = candles.takeLast(20).map { it.close }
            .reduce(BigDecimal::add)
            .divide(BigDecimal(20), 4, RoundingMode.HALF_EVEN)

        // 20일선 상향 돌파 시 매수 + 익절/손절을 함께 예약
        if (price > ma20 && !context.hasPosition("AAPL") && !context.hasOpenOrder("AAPL")) {
            return listOf(
                Signal.Buy(
                    symbol = "AAPL",
                    quantity = BigDecimal.ONE,
                    takeProfitPrice = price.multiply(BigDecimal("1.04")),
                    stopLossPrice = price.multiply(BigDecimal("0.98")),
                ),
            )
        }
        return emptyList()
    }
}
```

`@SpringBootApplication` 으로 실행하면 엔진이 전략 빈을 자동으로 찾아 스케줄링합니다.

## 동작 방식

- 엔진은 `pollInterval` 주기로 전략을 호출합니다. 기본적으로 **미국 정규장 시간에만** 호출됩니다 (`regularHoursOnly = false` 로 해제 가능)
- 매 틱마다 시세/캔들/계좌/보유/미체결 스냅샷(`StrategyContext`)을 만들어 전달합니다
- 전략이 반환한 `Signal` 은 엔진이 순서대로 실행합니다
  - `Signal.Buy` 에 `takeProfitPrice`/`stopLossPrice` 를 지정하면 체결 후 코어가 가격을 감시하다 자동 청산합니다 (**소프트웨어 브라켓** — 서버가 네이티브 BRACKET 주문을 지원하면 교체 예정)
  - `Signal.Sell` 은 보유 수량으로 자동 클램프됩니다 (공매도 방지)
  - 모든 주문에 `{전략이름}-{uuid}` 형식의 `clientOrderId` 가 부여됩니다 (24시간 중복 방지)
- 연속 실패가 임계치에 도달하면 **비상정지**: 미체결 전량 취소 + 신규 주문 차단 (`TradingGuard.resume()` 으로 해제)

## 규약

- 전략 안에서 API 를 직접 호출하거나 스레드를 만들지 않습니다 — 필요한 데이터는 `StrategyContext` 로 공급됩니다
- 전략 상태는 클래스 필드에 보관합니다 (인스턴스는 재사용됨). 단, 앱 재시작 시 소프트웨어 브라켓 상태는 사라지므로 `decide()` 에서 보유 포지션을 점검하는 로직을 두는 것을 권장합니다

## 수익률 확인

전략 앱을 띄우면 두 가지가 기본 제공됩니다:

- **`GET /pnl`** — 계좌 총평가/현금/평가손익/종목별 손익 JSON (`curl localhost:8080/pnl`)
- **주기 로그** — 기본 60분마다 `PNL / portfolio=... unrealized=+... | AAPL +54.32(+9.64%)` 형식으로 로그 출력

```yaml
hermetix:
  pnl:
    log-interval-minutes: 60   # 로그 주기 (enabled: false 로 끔)
    initial-capital: 20000     # 설정하면 총수익률(return=%)도 계산
```

## 현재 API 커버리지 (2026-08 기준)

| 기능 | 상태 |
|---|---|
| 토큰 발급/자동 갱신, 시세/캔들/캘린더/환율/종목, 계좌/보유/매수가능금액, 주문 생성·조회·취소, preview, 체결 내역 | ✅ 지원 |
| 고급 주문(STOP/BRACKET/OCO), modify, cancel-all, kill-switch, 거래한도 | ⏳ 서버 미배포 — 익절/손절은 소프트웨어 브라켓으로 대체 중 |

## 문서

- **[전략 작성 가이드](docs/strategy-guide.md)** — SPI 상세 레퍼런스, 패턴, 테스트, 트러블슈팅
- **[코어 아키텍처](docs/architecture.md)** — 내부 동작: 컴포넌트 맵, 틱 파이프라인, 상태 지도, 설계 결정
- [기여 가이드](CONTRIBUTING.md)

## 시작하기 / 공식 전략

| 레포 | 설명 |
|---|---|
| [next-strategy-template](https://github.com/tauthdev/next-strategy-template) | **여기서 시작하세요** — "Use this template" 으로 전략 개발 시작 |
| [next-larry-strategy](https://github.com/tauthdev/next-larry-strategy) | 변동성 돌파 (평균 몸통 1.2배 양봉 진입) |
| [next-trend-breakout-strategy](https://github.com/tauthdev/next-trend-breakout-strategy) | WMA 추세선 돌파 (가중 추세선 + 갭 돌파) |
| [next-grid-strategy](https://github.com/tauthdev/next-grid-strategy) | 목표가 스캘핑 (딥 매수 → 목표 감쇠 매도) |

## 커뮤니티 전략

직접 만든 전략을 공유하려면 [전략 공유 이슈](../../issues/new?template=strategy-share.md)를 올려주세요. 이 목록에 추가됩니다.

<!-- 커뮤니티 전략 목록 -->

## 라이선스

[MIT](LICENSE) — 이 프로젝트의 모든 것은 모의투자 학습용이며 투자 조언이 아닙니다.
