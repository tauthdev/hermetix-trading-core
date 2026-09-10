# 국내 증권사 오픈 API 실태 조사 (2026-09-10)

로드맵 Phase C 첫 항목("국내 증권사 REST 오픈API 실태 조사")의 결과. Hermetix 어댑터 관점(REST 여부·모의투자·인증·기능 범위·레이트리밋)으로 정리했다. 근거 URL 은 각 절에 있으며, 로그인 없이 확인하지 못한 항목은 "미확인" 으로 남겼다.

## 요약 — 어댑터 우선순위

| 순위 | 증권사 | 판정 | 핵심 근거 |
|---|---|---|---|
| 1 | **NH투자증권 (NH PLUG)** | 적합 | 2026-08-10 REST 정식 출시. 모의투자 서버 분리(`moapi.nhplug.com`), WebSocket 27채널, 미체결·체결 API, 공식 Python SDK(MIT) 로 스펙 역추적 가능. 국내·해외·파생·NXT 포함 |
| 2 | **DB증권** | 적합 | REST+WS, 모의투자 키 분리, KIS/LS 와 동형(앱키+토큰 24h). 국내·해외주식·선물옵션·채권. 공식 GitHub SDK |
| 3 | **LS증권** | 적합 | 2023-07 REST 정식. 모의투자 키 분리, REST 249 TR + WS 116 TR. 단 TR 코드 기반 요청이라 KIS 어댑터와 구조가 다르고 차트 TR 1 TPS 라 쓰로틀 필수 |
| 4 | **토스증권** | 조건부 | 2026-08-13 REST 정식(순수 OAuth2, OpenAPI JSON 제공, 국내+미국 단일 스키마). **모의투자 샌드박스 없음** → 실계좌 소액 검증 필요. 선물옵션 없음 |
| 5 | **KB증권** | 조건부 | 2026-07-20 개인 오픈베타. OAuth2 client credentials. **모의투자 "추후 제공"**, WebSocket·토큰 수명·rate limit 은 로그인 후 문서 확인 필요. 11월까지 스펙 변동 예고 |
| — | 미래에셋증권 | 관망 | 개인용 공개 API 없음. 2026-07 "트레이딩 Open API 개발" 채용공고 → 준비 중 추정. 분기별 재조사 |
| — | 삼성증권 | 부적합 | 기관 전용 유료 API 만. `openapi.ssfutures.com` 은 삼성선물(별도 법인) |
| — | 대신증권 | 부적합 | CYBOS/CREON Plus — Windows COM 전용, 32bit Python + HTS 상주. REST 없음(2025-09 요청 글만 존재) |
| — | 신한투자증권 | 부적합 | 개인용 증권 오픈API 공개 자료 없음. `openapi.shinhan.com` 은 그룹 B2B 제휴 포털 |

### 퀵스캔 (공개 REST 오픈API 유무)

| 증권사 | REST | 비고 |
|---|---|---|
| 하나증권 | 미확인 | HTS 화면 0789 "자체주문 API" + 설치형 모듈. 승인 1~3영업일 |
| 유진투자증권 | 없음 | 챔피언 Open API — Windows OCX/DLL |
| 유안타증권 | 없음 | 티레이더 Open API(2017) — Windows COM/DLL |
| 메리츠증권 | 없음(준비 중) | 내부 테스트 단계 보도(2026-08) |
| 카카오페이증권 | 없음 | 2022 개발자 문의 이후 출시 정보 없음 |
| 한화·교보·SK·현대차증권 | 미확인 | 공개 자료 검색 결과 없음 |

---

## 1. 토스증권 Open API

| 항목 | 내용 |
|---|---|
| 형태·상태 | REST(HTTP/JSON) + OAuth 2.0. 2026-05-21 사전신청 → **2026-08-13 정식**, 전 고객 개방 |
| 모의투자 | **공식 문서에 sandbox 언급 없음**. 커뮤니티: "별도 sandbox 미제공, 소액 실거래 테스트 권장" (공식 미확인) |
| 인증 | 앱/WTS 설정 → Open API 메뉴에서 `client_id/secret` 자체 발급(심사 없음). `POST https://openapi.tossinvest.com/oauth2/token` client_credentials. `Authorization: Bearer` + 계좌 API 는 `X-Tossinvest-Account` 헤더. 토큰 1시간(커뮤니티 자료) |
| 시장 | KRX 국내주식 + 미국주식(통합 API). 선물옵션 없음 |
| 기능 | 호가/현재가/체결/캔들, 종목마스터, 환율·휴장일, 계좌/보유, 주문 생성·정정·취소·목록·상세, 매수가능/매도가능/수수료. WebSocket `wss://openapi-ws.tossinvest.com/ws/v1`(체결틱·호가·주문이벤트, AsyncAPI 3.0) |
| Rate limit | 시세 10/s, 캔들 5/s, 주문 6/s(09:00–09:10 은 3/s), 계좌목록 1/s. 초과 시 429 + `Retry-After` (커뮤니티 정리, 미확인) |
| 자격·비용 | 계좌 보유 개인 누구나. API 비용 없음. 약관: 시세 상업적 이용·제3자 제공 금지 |
| 문서 | [developers.tossinvest.com](https://developers.tossinvest.com) (OpenAPI JSON, `llms.txt`). 비공식: PyPI/npm `tossinvest-openapi`, `tossinvest-mcp` |

근거: [developers.tossinvest.com/llms.txt](https://developers.tossinvest.com/llms.txt), [머니투데이 2026-08-13](https://www.mt.co.kr/stock/2026/08/13/2026081317374659887), [nbsp1221/tossinvest-openapi](https://github.com/nbsp1221/tossinvest-openapi)

**어댑터 적합성: 조건부.** 순수 REST/OAuth2 라 작성 비용은 가장 낮지만, 모의투자가 없어 Hermetix 의 "모의투자 통합" 포지셔닝과 어긋난다. Hermetix 가 실거래(Live) 프로파일을 지원하기로 결정한 뒤에 착수.

## 2. NH투자증권 — NH PLUG / N2 PLUG

| 항목 | 내용 |
|---|---|
| 형태·상태 | REST OpenAPI **2026-08-10 정식 출시**. 포털 [nhplug.com](https://www.nhplug.com/intro) (API 가이드·테스트베드·에러코드). 구 NH OpenAPI(32bit DLL) 는 2023-05-31 종료 |
| 모의투자 | **지원**. 운영 `https://api.nhplug.com:8443`, 모의 `https://moapi.nhplug.com:8443`(계좌구분 03). 토큰 발급은 운영 도메인만 |
| 인증 | 포털 API 사용신청 → AppKey/AppSecret → 접근토큰 발급/폐기 API. 토큰 24h. 고객번호 단위 통합 인증 |
| 시장 | 국내주식(KRX·NXT), 국내파생, 해외주식·파생, 채권, 금현물 |
| 기능 | 현재가/일별, 캔들(OHLCV), 잔고, 주문·정정·취소, 미체결, 체결. WebSocket 27채널(시세 7070·파생 7080), 세션당 10키, 앱키당 2세션 |
| Rate limit | 약 5건/s(SDK 는 4/s 쓰로틀), WS 전송 10/s. 초과 시 `IGW*` 코드 |
| 에러 | `rsp_cd`(예 `IGW40011`) + `rsp_msg`. **업무 오류가 HTTP 200 으로 내려오므로** `rsp_cd` 판정 필수 |
| 자격·비용 | 개인 가능. API 수수료 국내 0.01%·해외 0.09%. 자동매매 투자자 대상 명시 |
| 문서 | 공식 GitHub [PLUG-OpenAPI](https://github.com/PLUG-OpenAPI): `pip install nhplug`(MIT), nhplug-mcp |

근거: [디지털데일리 2026-08-10](https://www.ddaily.co.kr/page/view/2026081012073182832), [nhplug-sdk](https://github.com/PLUG-OpenAPI/nhplug-sdk), [testbed-howto](https://www.nhplug.com/testbed-howto)

**어댑터 적합성: 적합 (1순위).** 주의: 출시 1개월 차라 스펙 변동 가능, 비표준 포트(8443/7070/7080), HTTP 200 + `rsp_cd` 오류 매핑 필요, 초당 한도 낮음.

## 3. DB증권 (구 DB금융투자, 2025-04 사명 변경)

| 항목 | 내용 |
|---|---|
| 형태 | REST `https://openapi.dbsec.co.kr:8443` + WS 실전 `:7070` / 모의 `:17070` / 해외선물 `:7071` |
| 모의투자 | 지원. 모의투자용 APP_KEY 별도 발급(해외선물옵션은 모의 미지원). REST 도메인은 공유·키로 구분, WS 만 포트 분리 |
| 인증 | 홈페이지 인증서 로그인 → OpenAPI 신청 → 이메일로 APP_KEY/SECRET. 계정당 최대 3계좌. 키 유효기간 개인 1년/법인 3개월. 접근토큰 24h(익일 07시) |
| 시장 | 국내주식·국내선물옵션(야간 포함)·해외주식(미국)·해외선물옵션(실전만)·채권 |
| 기능 | 현재가/호가, 캔들(틱·분·일·주·월), 잔고·예수금·증거금, 주문/정정/취소, 미체결, 체결, WS 실시간(시세·주문·계좌) |
| Rate limit | 앱 레벨 20 TPS, 엔드포인트별 2~10 TPS |
| 자격·비용 | 개인·법인 가능. API 무료, 국내주식 수수료 0.01% 이벤트(~2026-12-31) |
| 문서 | [openapi.dbsec.co.kr](https://openapi.dbsec.co.kr/intro), 공식 [DBsecurities/dbsec-open-api](https://github.com/DBsecurities/dbsec-open-api) (Python SDK, 자동 토큰·유량제어) |

**어댑터 적합성: 적합 (2순위).** KIS 어댑터와 구조가 가장 비슷해 이식 비용이 낮다.

## 4. LS증권 (구 이베스트투자증권)

| 항목 | 내용 |
|---|---|
| 형태·상태 | 2023-07-25 웹 기반 OPEN API 정식. xingAPI(COM/DLL) 와 병행 |
| 모의투자 | 지원. 실전/모의 App Key 별도, 키에 따라 서버 자동 분기 |
| 인증 | `POST https://openapi.ls-sec.co.kr:8080/oauth2/token` client_credentials(`appkey`/`appsecretkey`). 토큰 **발급일 익일 07시까지** |
| 시장 | 국내주식·국내 선물/옵션·해외선물·해외주식(2024 추가) |
| 기능 | 현재가(t1102), 차트(t8410), 잔고, 주문/정정/취소, 미체결/체결, WS(별도 포트). REST 249 TR + WS 116 TR |
| Rate limit | TR 별 상이. 차트 TR 1 TPS. 2025-12 호출 한도 약 3배 확대. 초과 시 429 |
| 에러 | 응답 본문 `rsp_cd`("00000" 정상) / `rsp_msg` |
| 문서 | [openapi.ls-sec.co.kr](https://openapi.ls-sec.co.kr), 커뮤니티 [krsec(Go)](https://github.com/smallfish06/krsec), [LsApiHelper(Python)](https://github.com/xorrhks0216/LsApiHelper) |

**어댑터 적합성: 적합 (3순위).** 엔드포인트 하나에 `tr_cd` 헤더로 TR 을 구분하는 구조라 어댑터 내부에 TR 카탈로그가 필요하다.

## 5. KB증권

| 항목 | 내용 |
|---|---|
| 형태·상태 | B2B 용 Open API 를 **2026-07-20 개인 대상 오픈베타** 개방. 포털 [openapi.kbsec.com](https://openapi.kbsec.com/intro) |
| 모의투자 | **추후 제공 예정** (포털의 "모의거래 체험" 은 샘플 응답 체험) |
| 인증 | App Key/Secret → OAuth2 client credentials. 토큰 수명 미확인 |
| 시장·기능 | 국내주식 시세/주문(현재). 해외주식 주문·선물옵션은 11월까지 확대 예정. WS 미확인 |
| Rate limit·에러 | "API 별 상이" — 로그인 후 문서 확인 필요 |
| 문서 | 포털에서 Java/Python/JS 샘플 자동 생성. 공식 GitHub 없음 |

근거: [뉴시스 2026-08-04](https://www.newsis.com/view/NISX20260804_0003735986), [KB 블로그](https://blog.kbsec.com/about/kb%EC%A6%9D%EA%B6%8C-open-api-%EC%82%AC%EC%9A%A9%EB%B2%95/)

**어댑터 적합성: 조건부.** 계좌 개설 후 포털 로그인 문서(토큰 수명·WS·에러 스키마) 확보 + 모의투자 출시 뒤 착수.

## 6. 미래에셋·삼성·대신·신한

- **미래에셋증권**: 개인용 공개 API 없음. 2026-07-09 "트레이딩 Open API 개발 지원" 채용공고에서 REST/JSON 명세 작성 업무 확인 → 개발 중 추정. 2026-09 개인 오픈API 제공사 목록(한투·토스·KB·NH·DB)에 미포함. 출시 공지 모니터링.
- **삼성증권**: 기관투자자 대상 유료 API 만. 개인 경로 없음.
- **대신증권**: CYBOS Plus / CREON Plus — Windows COM 전용(VB/C#/Excel/Python 32bit), HTS 상시 실행 필수. 주문 15초당 20건. REST 없음. Windows 사이드카(COM→HTTP 브리지)를 직접 만들면 조건부 가능하나 Hermetix 의 "DB 없음·설정 한 줄" 원칙과 맞지 않는다.
- **신한투자증권**: 개인용 증권 오픈API 공개 자료 없음. 2026-08 오픈API 제공사 목록에 미포함.

---

## 공통 관찰

1. **2026년 여름에 REST 전환이 몰렸다** — KB(7월 베타), NH(8/10), 토스(8/13). 모두 OAuth2 client credentials + appkey/secret 패턴이라 Hermetix `BrokerClient` 와 인증 모델이 일치한다.
2. **모의투자 유무가 갈림길이다** — NH·DB·LS·KIS·키움은 모의 서버/키가 분리돼 있고, 토스·KB 는 없다(KB 는 예정). Hermetix 가 모의투자 통합을 표방하는 한 NH → DB → LS 순서가 자연스럽다.
3. **에러 형식이 두 갈래** — HTTP 상태 기반(토스·넥스트) vs HTTP 200 + `rsp_cd`(NH·LS·KIS·키움). 후자는 어댑터에서 본문 코드 판정이 필수다.
4. **레이트리밋이 전반적으로 낮다** (NH 5/s, LS 차트 1 TPS, 토스 계좌 1/s). 어댑터 단 쓰로틀(KIS 600ms 방식)을 표준 부품으로 빼는 것이 좋다.

## 다음 단계 제안

1. NH PLUG 모의계좌 발급 → 실측(토큰/시세/캔들/잔고/주문/에러) → `nh` 어댑터.
2. DB증권 모의 키 발급 → `db` 어댑터 (KIS 어댑터 복제 수준).
3. 어댑터 컨포먼스 테스트 킷(로드맵 Phase B)을 먼저 만들면 위 두 건의 기여 조건이 명확해진다.
4. 토스·KB 는 모의투자 출시 여부를 분기마다 재확인.
