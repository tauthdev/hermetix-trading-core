---
name: 새 브로커 요청
about: 지원했으면 하는 증권사 오픈 API 를 요청
title: "Broker: "
labels: broker-request
---

## 증권사

<!-- 예: NH투자증권 (NH PLUG) -->

- 공식 개발자 포털/문서 URL:
- API 형태: <!-- REST(HTTP/JSON) / WebSocket / Windows COM·DLL(OCX) — COM 전용이면 Hermetix 가 붙일 수 없습니다 -->

## 조사한 내용 (아는 만큼만)

| 항목 | 내용 |
|---|---|
| 모의투자(paper) 지원 | <!-- 지원 / 미지원 / 예정. 모의 서버 URL 이 분리돼 있는지 --> |
| 실전투자(live) 지원 | <!-- 개인 발급 가능 여부, 심사 필요 여부 --> |
| 인증 방식 | <!-- appkey/secret → 토큰, OAuth2 client credentials 등. 토큰 유효기간 --> |
| 시장 | <!-- KRX / 미국 / 선물옵션 등 --> |
| 기능 | <!-- 현재가 · 캔들(분/일) · 잔고 · 주문/정정/취소 · 미체결 · 체결 · 실시간 --> |
| 레이트리밋 | <!-- 초당 건수, 초과 시 응답(429 / 본문 코드) --> |
| 에러 형식 | <!-- HTTP 상태 기반인지, HTTP 200 + 본문 코드(rsp_cd 등)인지 --> |
| 비용·약관 | <!-- API 이용료, 자동매매 허용 여부 --> |

## 왜 필요한가요?

<!-- 이 브로커여야 하는 이유. 사용 중인 계좌, 원하는 전략 등 -->

## 도움 주실 수 있는 것

- [ ] 이 증권사 계좌와 API 키가 있어 **실측(토큰·시세·캔들·잔고·주문·에러 포맷)** 을 도울 수 있습니다
- [ ] 실측 응답으로 [컨포먼스 픽스처](../../blob/main/conformance/README.md)(`conformance/fixtures/<broker>.json`) 작성을 도울 수 있습니다
- [ ] 어댑터 구현 PR 을 직접 올리겠습니다 (Kotlin 레퍼런스 + Python/JS/Go 포팅은 한 세트입니다)

## 체크리스트

- [ ] [README 지원 브로커 표](../../blob/main/README.md#지원-브로커)와 열린 `broker-request` 이슈에 같은 증권사가 없는지 확인했습니다
- [ ] API 키·계좌번호 등 비밀값을 이슈에 적지 않았습니다
