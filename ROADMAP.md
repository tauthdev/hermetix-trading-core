# Hermetix 로드맵

목표는 **국내외 증권사 모의투자를 하나의 인터페이스로 통일한 연결 계층**이 되어, 그 위의 모든 봇/도구의 기반이 되는 것입니다.

지향하는 설계 원칙:

1. 어댑터가 하나 늘 때마다 모든 사용자가 이득을 보는 **네트워크 효과 구조**
2. 브로커마다 되는 것/안 되는 것을 코드로 선언하는 **capability 정직성**
3. 어느 브로커든 같은 방식으로 처리하는 **통일된 에러 체계**
4. "브로커 추가 = 레시피 + 표준 테스트 통과"인 **기여 플레이북**
5. 전략 엔진에 종속되지 않는 **독립적인 연결 계층** — 단, 연결 계층만 제공하는 프로젝트들과 달리 전략 엔진과 공식 전략을 함께 제공합니다 ("바로 돌아가는 봇"이 진입 장벽을 낮추는 무기)

---

## Phase A — 코어 재편 (0.5.x) ✅ 완료

- [x] `hermetix-broker` / `hermetix-engine` 모듈 분리 — 봇 없이 연결 계층만 쓰는 사용자(시세 수집, 대시보드, 알림) 지원
- [x] `BrokerCapabilities` 선언 — 캔들 주기, clientOrderId, 네이티브 브라켓 등을 코드로 선언하고 엔진이 적응. 기동 시 전략-브로커 호환성 검증 (fail-fast)
- [x] 에러 체계 통일 — `RateLimitError`(자동 백오프) / `MarketClosedError`(조용히 스킵, 비상정지 카운트 제외) / `InsufficientFundsError` / `InvalidOrderError` / `AuthError` / `OrderNotFoundError`
- [x] (0.6.0) 거래 환경 `paper | live` — 어댑터가 호스트·TR ID 를 고르고, LIVE 는 `hermetix.live.enabled` 명시 동의 없이는 기동 거부
- [x] (0.6.0) 주문 금액 상한 `RiskGuard` — 1건 / 일일 누적
- [x] (0.6.0) `MARKET:CODE` 심볼 접두 — 다중 시장 브로커 대비, 기존 전략 무변경

## Phase B — 커뮤니티 성장 엔진

- [x] 어댑터 컨포먼스 테스트 킷 — `conformance/` 골든 픽스처 + 네 언어 공통 시나리오 (`BrokerConformance` 는 test-fixtures 아티팩트로 배포). 새 어댑터는 통과가 기여 조건
- [x] 레이트리밋 공용 부품 `RateLimiter` — 쓰로틀 + 백오프 + `Retry-After`
- [x] 브로커 지원 매트릭스 (README 최상단)
- [x] "새 브로커 요청" 이슈 템플릿 (`.github/ISSUE_TEMPLATE/broker-request.md`)
- [ ] GitHub Actions CI (빌드 + 단위 테스트, 시크릿 보유 시 실서버 스모크)
- [x] 어댑터 추가 가이드 문서화 — `conformance/README.md` (실측 → 픽스처 → 구현 → 컨포먼스 → 등록)

## Phase C — 브로커 커버리지 확장

- [ ] 국내 증권사 REST 오픈API 실태 조사 (LS증권, 미래에셋, KB, 대신 등)
- [ ] 조사 결과 순서대로 어댑터 추가 (커뮤니티 기여 유도)
- [ ] 토스증권 — Open API 에 모의투자 샌드박스가 출시되면 추가
- [ ] 넥스트증권 `/v2` 연동: `POST /v2/orders/advanced`(BRACKET) 네이티브 브라켓 전환, `POST /v2/kill-switch` 를 `TradingGuard.halt()` 에 연결 (공개 스펙 v1.3 시점 서버 제공 확인)

## Phase D — 실시간 계층 (Hermetix Pro)

- [ ] 웹소켓 스트리밍 추상화 (next/KIS/키움 모두 WS 제공)
- [ ] 이벤트 기반 틱 — 폴링 대신 체결가 스트림으로 전략 트리거 (스캘핑류 품질 향상)

## Phase E — 다언어 도달 (진행 중)

- [x] Python / JavaScript·TypeScript / Go 네이티브 포팅 — Kotlin 레퍼런스를 언어별로 손 포팅하고 같은 골든 픽스처로 동작 일치를 보증한다
- 원칙: **증권사 키는 항상 사용자 기기에서만 쓰인다.** Hermetix 가 운영하는 서버로 키를 받아 대신 호출하는 구조(게이트웨이/데몬 호스팅)는 한국 금융 라이선스 문제로 채택하지 않는다. 다언어 지원은 ccxt 처럼 각 언어의 로컬 라이브러리로만 제공한다

---

버전 정책: SPI/BrokerClient 시그니처 변경 = minor 상승 (0.N.0). 태그 = 릴리즈 (JitPack). 상세 설계는 [docs/architecture.md](docs/architecture.md).
