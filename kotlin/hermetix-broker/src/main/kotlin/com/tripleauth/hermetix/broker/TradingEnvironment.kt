package com.tripleauth.hermetix.broker

/**
 * 거래 환경.
 *
 * - [PAPER]: 모의투자 — 기본값. 실제 돈이 움직이지 않는다
 * - [LIVE]: 실전투자 — 엔진은 `hermetix.live.enabled=true` 가 명시돼 있지 않으면 기동을 거부한다
 *
 * 환경은 어댑터 설정(`hermetix.<broker>.environment`)으로 정하고, 어댑터가 호스트·TR ID·키 형식을 그에 맞춘다.
 * 키는 항상 사용자 기기에서만 쓰인다 — Hermetix 는 어떤 서버로도 키를 보내지 않는다.
 */
enum class TradingEnvironment { PAPER, LIVE }
