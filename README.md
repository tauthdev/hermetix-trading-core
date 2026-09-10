<div align="center">

# Hermetix

**증권사 오픈 API 통합 트레이딩 프레임워크 — 모의투자로 검증하고, 설정 한 줄로 실전까지**

전략은 한 번만 작성하세요. 증권사도, 모의/실전도 설정 한 줄로 갈아끼웁니다. 키는 언제나 당신의 기기에서만 쓰입니다.

[![JitPack](https://jitpack.io/v/tauthdev/hermetix-trading-core.svg)](https://jitpack.io/#tauthdev/hermetix-trading-core)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Kotlin](https://img.shields.io/badge/Kotlin-1.9-7F52FF.svg?logo=kotlin&logoColor=white)](https://kotlinlang.org)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3-6DB33F.svg?logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)

[전략 작성 가이드](docs/strategy-guide.md) · [아키텍처](docs/architecture.md) · [컨포먼스 킷](conformance/README.md) · [로드맵](ROADMAP.md) · [기여하기](CONTRIBUTING.md)

</div>

---

Hermetix 는 **국내외 증권사 오픈 API** 를 하나의 `BrokerClient` 인터페이스로 통일합니다 (ccxt 가 거래소에 하는 일을 증권사에 합니다). 여기에 전략 실행 엔진까지 얹어서, `TradingStrategy` 인터페이스 하나만 구현하면 자동매매 봇이 완성됩니다. 모의투자로 검증한 봇은 설정 한 줄로 실전 계좌에 연결됩니다.

## 지원 브로커

| | ID | 증권사 | 시장 | 캔들 | 모의 | 실전 | 상태 |
|:---:|---|---|---|---|:---:|:---:|:---:|
| <img src="https://www.google.com/s2/favicons?domain=nextsecurities.com&sz=64" width="28"/> | `next` | [넥스트증권](https://docs.nextsecurities.dev/) | 미국주식 | 1m · 1d | ✅ | ✅ 키 프리픽스로 구분 | ✅ 검증 (공개 스펙 v1.3) |
| <img src="https://www.google.com/s2/favicons?domain=koreainvestment.com&sz=64" width="28"/> | `kis` | [한국투자증권](https://apiportal.koreainvestment.com/) | KRX 국내주식 | 1d | ✅ | ✅ 호스트·TR 자동 전환 | ✅ 검증 (모의) |
| <img src="https://www.google.com/s2/favicons?domain=kiwoom.com&sz=64" width="28"/> | `kiwoom` | [키움증권](https://openapi.kiwoom.com/) | KRX 국내주식 | 1d | ✅ | ✅ 호스트 자동 전환 | ✅ 검증 (모의) |
| <img src="https://www.google.com/s2/favicons?domain=nhqv.com&sz=64" width="28"/> | `nh` | [NH투자증권 NH PLUG](https://www.nhplug.com/) | KRX 국내주식 | 1d | ✅ 호스트 분리 | ✅ | ⚠️ 미검증 (문서 기반) |
| <img src="https://www.google.com/s2/favicons?domain=dbsec.co.kr&sz=64" width="28"/> | `db` | [DB증권](https://openapi.dbsec.co.kr/) | KRX 국내주식 | 1d | ✅ 키로 구분 | ✅ | ⚠️ 미검증 (문서 기반) |

✅ 검증 = 실서버 스모크 테스트(시세→캔들→계좌→주문 전 구간)를 통과한 환경. KIS·키움의 실전은 호스트·TR ID 전환만 구현돼 있고 실계좌 스모크는 아직입니다.
⚠️ 미검증 = 공식 SDK·문서에서 엔드포인트와 필드명을 역추적해 만든 어댑터. 네 언어 컨포먼스 시나리오는 통과했지만 픽스처가 실측이 아니라 문서 재구성값이라, 모의계좌 실측으로 확인되기 전까지는 스펙 해석 오류가 있을 수 있습니다. 실측을 도와주실 분은 [새 브로커 요청 이슈](../../issues/new?template=broker-request.md)로 알려주세요.
브로커별 지원 기능은 [`BrokerCapabilities`](kotlin/hermetix-broker/src/main/kotlin/com/tripleauth/hermetix/broker/BrokerCapabilities.kt) 로 코드에 선언되며(캔들 주기·지원 환경·시장·멱등키 등), 엔진이 기동 시 전략-브로커 호환성을 검증합니다.

**다음 어댑터 후보** ([2026-09 국내 증권사 오픈 API 조사](claudedocs/korean-broker-openapi-survey-2026-09.md)):

| 증권사 | 판정 | 비고 |
|---|---|---|
| LS증권 | 다음 | REST, 모의 지원. TR 코드 기반 |
| 토스증권 | 조건부 | 2026-08 REST 출시. **모의투자 없음** — 실전 전용 |
| KB증권 | 조건부 | 2026-07 개인 오픈베타. 모의투자 "추후" |
새 브로커를 원하시면 [새 브로커 요청 이슈](../../issues/new?template=broker-request.md)를 올려주세요 — 어댑터 기여는 [컨포먼스 킷](conformance/README.md) 절차(실측 픽스처 → 구현 → 네 언어 공통 시나리오 통과)를 따릅니다.

## 왜 Hermetix 인가

- **브로커 독립 전략** — 같은 전략 코드가 넥스트증권(미국)과 한국투자·키움(KRX)에서 그대로 돕니다
- **전략 = 클래스 하나** — 인증, 시세/계좌 조회, 주문 실행, 체결 추적은 전부 코어가 처리합니다
- **소프트웨어 브라켓** — `Signal.Buy(takeProfitPrice=…, stopLossPrice=…)` 한 줄로 익절/손절 자동화
- **안전 우선** — 매도 수량 자동 클램프(공매도 방지), 주문 멱등키, 연속 실패 시 비상정지(미체결 전량 취소 + 주문 차단), 주문 금액 상한
- **모의 → 실전 전환은 설정 한 줄** — `environment: live` 와 명시 동의(`hermetix.live.enabled`)가 있어야만 실전 주문이 나갑니다. 키는 항상 당신의 기기에서만 쓰입니다
- **레이트리밋 내장** — 증권사별 요청 제한을 공용 `RateLimiter` 가 쓰로틀/백오프/`Retry-After` 로 흡수합니다
- **네 언어, 같은 규약** — Kotlin 레퍼런스와 Python/JS/Go 포팅이 같은 골든 픽스처의 [컨포먼스 시나리오](conformance/README.md)를 통과합니다
- **KRX 호가단위 자동 보정** — 계산된 지정가를 KRX 가격대별 호가단위(1원~1,000원)에 맞게 어댑터가 보정합니다
- **수익률 기본 제공** — `GET /pnl` 엔드포인트와 주기 PnL 로그가 모든 봇에 자동 포함됩니다
- **타입화된 에러** — `MarketClosedError`, `RateLimitError`, `InsufficientFundsError`… 어느 브로커든 같은 방식으로 처리합니다

## 설치

언어별 네이티브 구현이 제공됩니다 — 같은 브로커/전략 규약, 같은 안전장치:

| 언어 | 폴더 | 버전 | 의존성 | 설치 |
|---|---|---|---|---|
| Kotlin/JVM (레퍼런스) | `kotlin/` | 0.6.0 | Spring Boot | JitPack (아래) |
| [Python](python/) | `python/` | 0.2.0 | 0개 (stdlib, 3.10+) | `pip install ./python` |
| [JavaScript/TypeScript](js/) | `js/` | 0.2.0 | decimal.js (Node 18+) | `npm install ./js` |
| [Go](go/) | `go/` | — | shopspring/decimal | `go get github.com/tauthdev/hermetix-trading-core/go` |

Python/JS 는 아직 PyPI/npm 에 올리지 않아 레포 경로로 설치합니다.

**Kotlin/JVM**:

```kotlin
// settings.gradle.kts
repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

// build.gradle.kts
dependencies {
    // 전략 봇: engine (연결 계층이 함께 딸려옴)
    implementation("com.github.tauthdev.hermetix-trading-core:hermetix-engine:0.6.0")

    // 봇 없이 연결 계층만 (시세 수집, 대시보드, 알림봇 등):
    // implementation("com.github.tauthdev.hermetix-trading-core:hermetix-broker:0.6.0")
}
```

## 빠른 시작 — 전략 봇

> 처음이라면 [hermetix-strategy-template](https://github.com/tauthdev/hermetix-strategy-template) 을 "Use this template" 으로 복제하는 게 가장 빠릅니다.

```yaml
# application.yml
hermetix:
  broker: next                   # next | kis | kiwoom
  next:
    client-id: pk_test_...
    client-secret: sk_test_...
    account-id: acc_main
```

```kotlin
@Component
class MyStrategy : TradingStrategy {

    override val spec = StrategySpec(
        name = "my-first",
        symbols = listOf("AAPL"),
        candleInterval = CandleInterval.DAY_1,
        candleLimit = 20,
    )

    override fun decide(context: StrategyContext): List<Signal> {
        val price = context.quote("AAPL")?.price ?: return emptyList()
        val ma20 = context.candles("AAPL").map { it.close }
            .reduce(BigDecimal::add).divide(BigDecimal(20), 4, RoundingMode.HALF_EVEN)

        if (price > ma20 && !context.hasPosition("AAPL") && !context.hasOpenOrder("AAPL")) {
            return listOf(
                Signal.Buy(
                    symbol = "AAPL",
                    quantity = BigDecimal.ONE,
                    takeProfitPrice = price.multiply(BigDecimal("1.04")),  // 익절/손절은
                    stopLossPrice = price.multiply(BigDecimal("0.98")),    // 코어가 자동 실행
                ),
            )
        }
        return emptyList()
    }
}
```

`@SpringBootApplication` 으로 실행하면 엔진이 전략 빈을 찾아 해당 시장의 정규장 시간에만 호출합니다.

증권사를 바꿀 땐 설정만 수정합니다:

```yaml
hermetix:
  broker: kis                    # 이 한 줄이 전부
  kis:
    appkey: ${KIS_APPKEY:}
    appsecret: ${KIS_APPSECRET:}
    cano: ${KIS_CANO:}           # 모의계좌번호 8자리

# 키움이라면
#  broker: kiwoom
#  kiwoom:
#    appkey: ${KIWOOM_APPKEY:}
#    secretkey: ${KIWOOM_SECRETKEY:}

# NH투자증권 NH PLUG (⚠️ 미검증) — 계좌번호를 비우면 모의(acct_type=03) 계좌를 자동 선택
#  broker: nh
#  nh:
#    app-key: ${NH_APP_KEY:}
#    app-secret: ${NH_APP_SECRET:}
#    account-no: ${NH_ACCOUNT_NO:}

# DB증권 (⚠️ 미검증) — 모의투자용 키를 넣으면 모의, 실전 키면 실전 (호스트 동일)
#  broker: db
#  db:
#    app-key: ${DB_APP_KEY:}
#    app-secret: ${DB_APP_SECRET:}
```

## 실전투자로 전환

모의투자에서 검증한 봇을 실제 계좌로 옮길 때는 두 가지를 명시해야 합니다. 하나라도 빠지면 엔진이 기동을 거부합니다.

```yaml
hermetix:
  broker: kis
  kis:
    environment: live          # paper(기본) | live — 호스트·TR ID 가 자동으로 바뀝니다
    appkey: ${KIS_APPKEY:}
    appsecret: ${KIS_APPSECRET:}
    cano: ${KIS_CANO:}
  live:
    enabled: true              # 실전 명시 동의. 없으면 "LIVE 설정됨 - 기동하지 않음" 으로 멈춥니다
  risk:
    max-order-value: 1000000   # 주문 1건 상한 (브로커 통화). 넘는 시그널은 제출하지 않습니다
    max-daily-order-value: 5000000   # 하루(UTC) 누적 상한 — 매수·매도 합산
```

- 넥스트증권은 키 프리픽스가 환경을 결정합니다 (`pk_test_`=모의, `pk_live_`=실전). `environment` 와 키가 어긋나면 기동 시 실패합니다
- 주문 금액 상한은 모의투자에서도 설정하면 적용됩니다. 현재가를 알 수 없는 종목의 주문은 상한이 설정된 경우 거부됩니다
- 심볼에 시장 접두를 붙일 수 있습니다 (`KRX:005930`, `US:AAPL`). 접두 없는 심볼은 브로커 기본 시장으로 해석되므로 기존 전략은 그대로 동작합니다
- **키는 절대 Hermetix 가 운영하는 어떤 서버로도 전송되지 않습니다.** 라이브러리가 당신의 기기에서 증권사를 직접 호출합니다

## 빠른 시작 — 연결 계층만

봇이 필요 없다면 `hermetix-broker` 만으로 통일 API 를 사용할 수 있습니다. Spring 컨테이너도 필요 없습니다:

```kotlin
val broker: BrokerClient = KisApiClient(
    KisApiProperties(appkey = "...", appsecret = "...", cano = "..."),
    objectMapper,
)

broker.getQuotes(listOf("005930")).quotes[0].price    // 삼성전자 현재가
broker.getCandles("005930", CandleInterval.DAY_1, 30) // 일봉 30개
broker.getHoldings()                                  // 보유 포지션
```

브로커를 `NextApiClient` 나 `KiwoomApiClient` 로 바꿔도 **호출 코드는 동일**합니다 — 응답의 방언(부호 접두, zero-padding, TR-ID 체계)은 어댑터가 전부 정규화합니다.

## 동작 방식

```
전략 (TradingStrategy)          ← 당신이 작성하는 유일한 부분
    ↓ Signal (Buy/Sell/Cancel)
StrategyEngine                  ← 정규장 스케줄링, 컨텍스트 구성, 브라켓/비상정지
    ↓ BrokerClient 인터페이스
next / kis / kiwoom 어댑터       ← 인증, 레이트리밋, 방언 정규화
```

- 엔진은 `pollInterval` 주기로 전략을 호출합니다 — 해당 브로커 시장의 정규장에만 (next=미국장 ET, kis/kiwoom=KRX KST)
- 기동 시 검증: 전략의 캔들 주기를 브로커가 지원하는지, 브로커 환경(모의/실전)이 선언된 것인지, 실전이면 명시 동의가 있는지 — 하나라도 어긋나면 스케줄하지 않습니다
- `Signal.Sell` 은 보유 수량으로 자동 클램프됩니다 (공매도 방지). 주문 금액 상한(`hermetix.risk.*`)을 넘는 시그널은 제출하지 않습니다
- 익절/손절(소프트웨어 브라켓)은 앱 메모리에서 관리됩니다 — 재시작 시 사라지므로 [전략 가이드](docs/strategy-guide.md)의 복원 패턴을 참고하세요
- 연속 실패가 임계치(기본 5회)에 도달하면 비상정지 — 미체결 전량 취소 후 주문 차단 (휴장·레이트리밋은 카운트 제외)
- 심볼은 `MARKET:CODE` 접두를 허용합니다 (`KRX:005930`, `US:AAPL`). 어댑터는 코드만 보내고, 컨텍스트 조회는 접두 유무를 무시합니다

## 수익률 확인

봇을 띄우면 자동으로 제공됩니다:

- **`GET /pnl`** — 총평가/현금/평가손익/종목별 손익 JSON
- **주기 로그** — `PNL / portfolio=21363.45 unrealized=+271.73 | AAPL +54.32(+9.64%) …`

```yaml
hermetix:
  pnl:
    log-interval-minutes: 60
    initial-capital: 20000     # 설정하면 총수익률(return=%)도 계산
```

## 공식 전략

| 레포 | 전략 | 특징 |
|---|---|---|
| [hermetix-strategy-template](https://github.com/tauthdev/hermetix-strategy-template) | 이동평균 예제 | **여기서 시작하세요** |
| [hermetix-larry-strategy](https://github.com/tauthdev/hermetix-larry-strategy) | 변동성 돌파 | 캔들 분석 + 손절 브라켓 |
| [hermetix-trend-breakout-strategy](https://github.com/tauthdev/hermetix-trend-breakout-strategy) | WMA 추세선 돌파 | 지표 계산 + 익절/손절 브라켓 |
| [hermetix-grid-strategy](https://github.com/tauthdev/hermetix-grid-strategy) | 목표가 스캘핑 | 지정가/취소 컨트롤, KRX 호환 |

직접 만든 전략을 공유하려면 [전략 공유 이슈](../../issues/new?template=strategy-share.md)를 올려주세요.

<!-- 커뮤니티 전략 목록 -->

## 문서

- [전략 작성 가이드](docs/strategy-guide.md) — SPI 레퍼런스, 패턴, 안전장치, 테스트, 트러블슈팅
- [아키텍처](docs/architecture.md) — 모듈 구조, 틱 파이프라인, 거래 환경·RiskGuard·심볼 규약, 어댑터 비교표, 상태 지도
- [컨포먼스 킷](conformance/README.md) — 새 어댑터 검증 시나리오와 네 언어 공용 골든 픽스처
- [국내 증권사 오픈 API 조사 (2026-09)](claudedocs/korean-broker-openapi-survey-2026-09.md) — 다음 어댑터 우선순위 근거
- [로드맵](ROADMAP.md) — 5단계 발전 계획
- [기여 가이드](CONTRIBUTING.md)

## 라이선스

[MIT](LICENSE)

> **면책**: 이 프로젝트는 학습·연구용 소프트웨어이며 투자 조언이 아닙니다. 실전투자(`environment: live`)로 발생하는 모든 손실의 책임은 사용자에게 있습니다 — 모의투자에서 충분히 검증하고, 주문 금액 상한을 설정한 뒤 소액으로 시작하세요. Hermetix 는 어떤 서버로도 당신의 API 키를 받지 않습니다. 각 증권사 로고는 해당 회사의 자산이며, 지원 서비스를 표시하기 위해서만 사용됩니다.
