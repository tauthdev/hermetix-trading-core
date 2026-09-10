# 코어 아키텍처

이 문서는 hermetix-trading-core 의 **내부 동작**을 설명합니다. 전략을 작성하려면 [전략 작성 가이드](strategy-guide.md)를 보세요 — 이 문서는 코어에 기여하거나 동작을 깊이 이해하려는 사람을 위한 것입니다.

## 설계 원칙

1. **전략 작성자의 표면적 최소화** — 사용자가 만지는 것은 `TradingStrategy` / `StrategyContext` / `Signal` 세 타입뿐. 나머지는 전부 자동설정으로 숨긴다. 이 세 타입의 시그니처 변경은 모든 전략 레포를 깨뜨리므로 신중히 다룬다
2. **DB 없음, 서버가 source of truth** — 보유/미체결/체결은 항상 모의투자 서버에서 조회한다. 커뮤니티 사용자가 DB 설정 없이 `yml + 전략 클래스` 만으로 봇을 띄우는 것이 목표. 대가로 일부 상태(브라켓, 감쇠 카운터 등)는 메모리에만 있어 재시작 시 사라진다 — 이 트레이드오프는 각 지점에 문서화한다
3. **안전 우선** — 공매도 방지(매도 클램프), 주문 멱등성(clientOrderId), 연속 실패 시 자동 정지. 모의투자라도 폭주하는 봇은 커뮤니티 신뢰를 깎는다
4. **호스트 앱을 오염시키지 않는다** — 코어의 Jackson 설정, 스케줄러 스레드는 전부 내부 전용. 앱의 전역 빈을 건드리지 않는다 (0.2.1 에서 ObjectMapper 빈 노출을 제거한 이유)

## 레포 구조 (언어별 모노레포)

```
kotlin/            <- 레퍼런스 구현 (여기서 브로커 변경을 먼저 실측/수정한다)
  hermetix-broker  <- 연결 계층: BrokerClient/Capabilities/에러 계층 + next/kis/kiwoom 어댑터
                      Spring 컨테이너 없이도 사용 가능 (어댑터는 일반 생성자 주입)
  hermetix-engine  <- 전략 계층: 전략 SPI + 실행 엔진 + PnL + 자동설정 (broker 에 api 의존)
python/            <- Python 구현 (stdlib 만)
js/                <- TypeScript 구현 (decimal.js)
go/                <- Go 구현 (shopspring/decimal)
                      * 모든 포트는 실측 골든 픽스처 재생 테스트로 레퍼런스와 동작 일치를 보증한다
jitpack.yml        <- JitPack 이 kotlin/ 에서 빌드하도록 지정
```

의존성 좌표: `com.github.tauthdev.hermetix-trading-core:hermetix-engine` (봇) 또는 `:hermetix-broker` (연결만).

## 컴포넌트 맵

```
                    NextTradingAutoConfiguration (자동설정 진입점)
                                   │
      ┌──────────────┬─────────────┼──────────────┬─────────────┐
      ▼              ▼             ▼              ▼             ▼
 TokenManager → NextApiClient  MarketCalendar  PnlService   TradingGuard
 (토큰 캐시/갱신)  (전 API 래핑)    Service        │  ├ PnlLogger (주기 로그)
                     ▲          (개장 판단,      │  └ PnlController (GET /pnl)
                     │           6h 캐시)       │
                     │                          │
                 StrategyEngine ────────────────┘
                 (전략별 스케줄 루프)
                     │
              ┌──────┴──────┐
              ▼             ▼
        OrderExecutor   BracketMonitor
        (Signal→주문)    (소프트웨어 익절/손절)
```

- 모든 빈은 `@ConditionalOnMissingBean` — 앱이 같은 타입의 빈을 정의하면 코어 구현을 교체할 수 있다
- `StrategyEngine` 은 `hermetix.engine.enabled=false` 로 끌 수 있다 (API 클라이언트만 쓰는 용도)

## 인증 흐름 (TokenManager)

```
getToken()
 ├─ 캐시 토큰이 있고 만료까지 margin(기본 60초) 이상 남음 → 그대로 반환
 └─ 아니면 @Synchronized refresh()
      └─ POST /v1/oauth/token (client_credentials) → 캐시 갱신
```

- `NextApiClient` 는 모든 인증 호출을 `executeWithRetry` 로 감싼다: 401(또는 `type=authentication`) 응답이면 `invalidate()` 후 **정확히 1회** 재발급-재시도. 재시도도 실패하면 예외 전파
- 토큰 유효기간은 공개 스펙 v1.3 기준 12시간(`expires_in=43200`), refresh 토큰 없음. 어댑터는 응답의 `expires_in` 을 그대로 신뢰한다
- 토큰 발급 API 만 에러 형식이 다르다 — 400/401 은 OAuth 표준 `{error, error_description}`, 429/5xx 는 플랫폼 엔벨로프. `TokenManager` 가 둘 다 `AuthError` 로 변환한다

### 넥스트증권 공통 헤더 (공개 스펙 v1.3)

| 헤더 | 대상 | 규칙 |
|---|---|---|
| `Authorization: Bearer {token}` | 전 API | 12h 토큰 |
| `X-Request-Id` | 토큰 발급 외 **전 고객 API 필수** | 영숫자·`.`·`_`·`-` 만, 64자 이하. 누락 시 400 `request-id-required`. 어댑터가 호출마다 `hmx-{uuid}` 를 생성하며, 에러 엔벨로프의 `requestId` 로 되돌아온다 |
| `X-Next-Account-Id` | 계좌·자산·주문 API 필수 (`GET /v1/account` 포함) | v1.1 의 `X-Nextsecurities-Account` 에서 개명. 토큰의 계좌와 불일치 시 403 `account-mismatch` |

## 엔진 틱 파이프라인 (StrategyEngine.tick)

엔진은 기동 시 모든 `TradingStrategy` 빈을 찾아 각자의 `pollInterval` 로 `scheduleWithFixedDelay` 등록한다. 스케줄러는 **poolSize 1** — 전략이 여러 개여도 틱은 순차 실행된다 (같은 계좌를 공유하므로 동시 주문 경합을 원천 차단).

```
tick(strategy):
 1. TradingGuard.isHalted → 즉시 반환
 2. spec.regularHoursOnly && !MarketCalendarService.isRegularOpen() → 반환
 3. buildContext(): quotes(1회) + candles(심볼당 1회) + account + holdings + orders + buyingPower
 4. BracketMonitor.check(context) → 청산 시그널이 있으면 먼저 실행   ← 전략보다 우선
 5. strategy.decide(context) → 반환된 시그널을 OrderExecutor 로 실행
 6. 성공 → guard.recordSuccess() / 예외 → guard.recordFailure() (임계치 도달 시 비상정지)
```

- 3단계의 API 호출 수 = `2 + 심볼 수 + 2` (quotes 는 다심볼 일괄). `candleLimit`/심볼 수가 틱 비용을 결정한다
- 예외는 틱 단위로 격리된다 — 한 틱이 실패해도 다음 틱은 정상 진행

## 주문 실행 (OrderExecutor)

| Signal | 처리 |
|---|---|
| `Buy` | `POST /v1/orders`. 성공 시 tp/sl 가격이 있으면 BracketMonitor 에 등록 |
| `Sell` | 수량을 `min(요청, 보유)` 로 클램프 (공매도 방지). 0 이면 스킵+경고 |
| `Cancel` | `DELETE /v1/orders/{id}` |

- 모든 주문의 `clientOrderId` = `{전략이름}-{uuid8}` → 서버가 24h 중복 제출을 거부 (엔진 재시도/중복 틱에 대한 안전망)
- 시그널 하나의 실패는 로그만 남기고 다음 시그널을 계속 실행한다 (부분 실패 허용)
- 비상정지 중에는 모든 시그널을 스킵한다

## 소프트웨어 브라켓 (BracketMonitor)

서버의 네이티브 BRACKET 주문(`POST /v2/orders/advanced`, 공개 스펙 v1.3 시점 `/v2` 로 제공)을 아직 연동하지 않아 코어가 소프트웨어로 대체한다:

```
등록: Buy 주문 접수 직후 {entryOrderId, symbol, qty, tp?, sl?} 저장 (메모리 Map)
매 틱 check():
 ├─ 미활성 브라켓: 진입 주문 상태 조회
 │    ├─ FILLED → 활성화
 │    └─ CANCELED/REJECTED/EXPIRED → 브라켓 폐기
 └─ 활성 브라켓: 현재가 vs tp/sl
      └─ 도달 → 브라켓 제거 후 시장가 Sell 시그널 생성 (수량은 보유로 클램프)
```

알려진 제약 (의도된 트레이드오프):
- **재시작 시 소실** — DB 없음 원칙의 대가. 전략 가이드에서 "재시작 후 포지션 수동 점검"을 권고
- 청산이 시장가라 급변동 시 슬리피지 존재
- 판정 주기가 엔진 틱 주기와 같으므로 틱 사이의 순간 스파이크는 놓칠 수 있다

네이티브 전환 로드맵: `OrderExecutor.buy()` 에서 tp/sl 존재 시 `/v2/orders/advanced` BRACKET 주문으로 보내는 fast-path 를 추가하고(`BrokerCapabilities.nativeBracket=true`), BracketMonitor 는 폴백으로 강등한다.

## 비상정지 (TradingGuard)

서버 킬 스위치(`/v2/kill-switch`, v1.3 시점 `/v2` 로 제공)를 아직 연동하지 않아 클라이언트 측에서 같은 효과를 낸다:

- 틱 연속 실패가 `hermetix.engine.max-consecutive-failures`(기본 5) 도달 → `halt()`
- `halt()`: 미체결 전량 개별 취소 + `halted=true` (이후 모든 주문 차단, 틱 스킵)
- 해제: `TradingGuard.resume()` 호출 또는 앱 재시작. **자동 해제는 없다** — 사람이 원인을 보게 만드는 것이 의도
- 연동 로드맵: `halt()` 에서 `POST /v2/kill-switch` 를 함께 호출해 서버 측에서도 주문을 차단(423 `trading-halted`)하도록 확장 예정

## 상태 지도 — 무엇이 어디에 있는가

| 상태 | 위치 | 재시작 시 |
|---|---|---|
| 보유 포지션, 미체결 주문, 체결 내역, 평균단가 | **서버** | 유지 (API 로 재조회) |
| 액세스 토큰 | 메모리 (TokenManager) | 재발급 (자동) |
| 시장 캘린더 | 메모리 캐시 (6h TTL) | 재조회 (자동) |
| 브라켓 (익절/손절 예약) | 메모리 (BracketMonitor) | **소실** |
| 비상정지 플래그, 연속 실패 카운터 | 메모리 (TradingGuard) | 초기화 (정지 해제됨) |
| 전략 내부 상태 (진입 시각, 감쇠 카운터 등) | 메모리 (전략 필드) | **소실** — 전략이 서버 상태로 복원하는 패턴 권장 |

## 에러 처리 계층 (0.5.0+)

어댑터는 브로커별 에러를 타입화된 계층으로 매핑하고, 엔진은 타입별로 반응한다:

```
BrokerApiException (기반)
 |- AuthError               -> 어댑터가 토큰 재발급 후 재시도 (넥스트), 소진 시 틱 실패
 |- RateLimitError          -> 어댑터가 백오프 재시도, 소진 시 틱 스킵 (비상정지 카운트 제외)
 |- MarketClosedError       -> 틱 조용히 스킵 (비상정지 카운트 제외 - KRX 합성 캘린더의 공휴일 케이스 포함)
 |- InsufficientFundsError  -> 해당 시그널만 스킵
 |- InvalidOrderError       -> 시그널 실패 로그
 |- OrderNotFoundError      -> 호출부 판단
```

## 브로커 어댑터

`BrokerClient` 구현체는 `hermetix.broker` 값으로 선택된다 (@ConditionalOnProperty). 실측 기반 어댑터별 특성:

| | next | kis | kiwoom |
|---|---|---|---|
| 인증 | OAuth client_credentials, 토큰 12h (v1.3) | appkey/appsecret → 토큰 24h (발급 1회/분 제한) | appkey/secretkey → 토큰 (expires_dt) |
| 레이트리밋 | 그룹별 초당 제한, 429 + `Retry-After` (어댑터 자동 재시도 없음 — 엔진이 다음 틱까지 대기) | 초당 제한 → 600ms 쓰로틀 + EGW00201 재시도 | TR당 초당 1회 → 1100ms 쓰로틀 + 재시도 |
| 캔들 | 1m/1d (v1.3) | 1d (분봉 API 가 당일 한정이라 미지원) | 1d |
| 캘린더 | 서버 제공 (미국장) | KRX 합성 (공휴일 미반영) | KRX 합성 |
| clientOrderId | 지원 (24h 멱등) | 미지원 (무시) | 미지원 (무시) |
| 미체결 조회 | 서버 제공 | **서버 미제공 → 어댑터 메모리 추적** (체결은 보유수량 변화로 근사, 재시작 시 추적 소실) | 서버 제공 (ka10075) |
| 주문취소 | orderId 만으로 가능 | ODNO 단독 (지점번호 불필요 - 실측) | 미체결 조회로 종목코드 역참조 |
| 수량/금액 표기 | JSON 문자열 | 문자열 | 부호 접두(가격) / zero-padded(금액) — 어댑터가 정규화 |
| 응답 정규화 | v1.3 원시 응답(quotes `outcome`, 캔들 `time`, `cashAmount`, `averageBuyPrice`, 캘린더 `status`+`sessions[]`)을 공통 모델로 변환. 등락률·손익률 %→비율, KST 세션 시각→뉴욕 현지 HH:mm, 총평가=예수금+보유 평가금액(보유 조회 1회 추가) | KIS 응답 → 공통 모델 | 키움 응답 → 공통 모델 |

새 어댑터 추가 절차: ① 모의서버 실측(토큰/시세/캔들/잔고/주문/에러 포맷) ② `BrokerClient` 구현 ③ 오토컨피그에 @ConditionalOnProperty 등록 ④ env-gated 실서버 스모크 테스트.

## 버전/호환 정책

- SPI(`TradingStrategy`/`StrategyContext`/`Signal`) 변경 = breaking → minor 버전 상승 (0.x 에서는 0.N+1.0)
- JitPack 이 git 태그를 빌드하므로 **태그 = 릴리즈**. 태그를 옮겨 달지 않는다 (JitPack 은 한 번 빌드한 버전을 캐시)
- Gradle Wrapper 는 반드시 커밋에 포함한다 (없으면 JitPack 이 구버전 Gradle 로 빌드 실패)

## 알려진 한계 요약

- 브라켓/전략 상태의 메모리 휘발성 (위 상태 지도 참조)
- 호가 스냅샷 기반 — L2 오더북/실시간 스트림 없음, 틱 주기 사이의 가격은 못 본다
- 정규장 판정은 캘린더 API 기준 — 프리/애프터마켓 주문은 `regularHoursOnly=false` 로 가능하나 체결 규칙은 서버 정책을 따른다
- 단일 계좌 전제 — 전략 여러 개가 같은 심볼을 다루면 보유/미체결 판단이 겹친다 (전략 가이드에서 금지 권고)
