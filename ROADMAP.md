# Hermetix 로드맵

롤모델은 [CCXT](https://github.com/ccxt/ccxt) 입니다 — 100+ 거래소를 하나의 인터페이스로 통일한 연결 계층이 되어, 그 위의 모든 봇/도구의 기반이 된 프로젝트. Hermetix 는 같은 구조를 **국내외 증권사 모의투자**에 적용합니다.

CCXT 에서 가져오는 성공 공식:

1. 어댑터가 하나 늘 때마다 모든 사용자가 이득을 보는 **네트워크 효과 구조**
2. 브로커마다 되는 것/안 되는 것을 코드로 선언하는 **capability 정직성**
3. 어느 브로커든 같은 방식으로 처리하는 **통일된 에러 체계**
4. "브로커 추가 = 레시피 + 표준 테스트 통과"인 **기여 플레이북**
5. 전략 엔진에 종속되지 않는 **독립적인 연결 계층**

단, CCXT 와 달리 전략 엔진과 공식 전략을 함께 제공합니다 — "바로 돌아가는 봇"이 진입 장벽을 낮추는 무기이기 때문입니다.

---

## Phase A — 코어 재편 (0.5.x) 🚧 진행 중

- [x] `hermetix-broker` / `hermetix-engine` 모듈 분리 — 봇 없이 연결 계층만 쓰는 사용자(시세 수집, 대시보드, 알림) 지원
- [x] `BrokerCapabilities` 선언 — 캔들 주기, clientOrderId, 네이티브 브라켓 등을 코드로 선언하고 엔진이 적응. 기동 시 전략-브로커 호환성 검증 (fail-fast)
- [x] 에러 체계 통일 — `RateLimitError`(자동 백오프) / `MarketClosedError`(조용히 스킵, 비상정지 카운트 제외) / `InsufficientFundsError` / `InvalidOrderError` / `AuthError` / `OrderNotFoundError`

## Phase B — 커뮤니티 성장 엔진

- [ ] 어댑터 컨포먼스 테스트 킷 — 새 어댑터는 표준 테스트 스위트 통과가 기여 조건
- [ ] 브로커 지원 매트릭스 (README 최상단) + "새 브로커 요청" 이슈 템플릿
- [ ] GitHub Actions CI (빌드 + 단위 테스트, 시크릿 보유 시 실서버 스모크)
- [ ] 어댑터 추가 가이드 문서화 (실측 → 구현 → 컨포먼스 → 등록 레시피)

## Phase C — 브로커 커버리지 확장

- [ ] 국내 증권사 REST 오픈API 실태 조사 (LS증권, 미래에셋, KB, 대신 등)
- [ ] 조사 결과 순서대로 어댑터 추가 (커뮤니티 기여 유도)
- [ ] 토스증권 — Open API 에 모의투자 샌드박스가 출시되면 추가
- [ ] 서버 신규 기능 추적: 넥스트증권 advanced 주문/kill-switch 배포 시 네이티브 전환

## Phase D — 실시간 계층 (Hermetix Pro)

- [ ] 웹소켓 스트리밍 추상화 (next/KIS/키움 모두 WS 제공)
- [ ] 이벤트 기반 틱 — 폴링 대신 체결가 스트림으로 전략 트리거 (스캘핑류 품질 향상)

## Phase E — 도달 범위 확장 (장기)

- [ ] `hermetix-server` — 코어를 데몬으로 띄워 통일 REST/WS 게이트웨이 노출 (파이썬/JS 사용자 지원, 트랜스파일 없이 다언어 도달)

---

버전 정책: SPI/BrokerClient 시그니처 변경 = minor 상승 (0.N.0). 태그 = 릴리즈 (JitPack). 상세 설계는 [docs/architecture.md](docs/architecture.md).
