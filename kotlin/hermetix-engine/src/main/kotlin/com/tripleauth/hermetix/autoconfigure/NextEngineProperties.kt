package com.tripleauth.hermetix.autoconfigure

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "hermetix.engine")
data class NextEngineProperties(
    /** false 면 전략 엔진을 기동하지 않는다 (API 클라이언트만 사용) */
    val enabled: Boolean = true,
    /** 연속 실패가 이 횟수에 도달하면 비상정지(미체결 전량 취소 + 신규 주문 차단) */
    val maxConsecutiveFailures: Int = 5,
)

/**
 * 실전투자 게이트. 브로커가 LIVE 환경으로 설정돼 있어도 `enabled=true` 가 없으면 엔진은 전략을 스케줄하지 않는다.
 * "모의인 줄 알고 실전 키를 넣은" 사고를 막기 위한 명시적 동의다.
 */
@org.springframework.boot.context.properties.ConfigurationProperties(prefix = "hermetix.live")
data class HermetixLiveProperties(
    val enabled: Boolean = false,
)

/** 주문 금액 상한 ([com.tripleauth.hermetix.engine.RiskGuard]). 브로커 통화 기준. 미설정 시 검사하지 않는다 */
@org.springframework.boot.context.properties.ConfigurationProperties(prefix = "hermetix.risk")
data class HermetixRiskProperties(
    /** 주문 1건의 추정 금액(수량 × 가격) 상한 */
    val maxOrderValue: java.math.BigDecimal? = null,
    /** 하루(UTC) 누적 주문 금액 상한 — 매수·매도 합산 */
    val maxDailyOrderValue: java.math.BigDecimal? = null,
)

@org.springframework.boot.context.properties.ConfigurationProperties(prefix = "hermetix.pnl")
data class NextPnlProperties(
    /** false 면 PnL 주기 로그를 끈다 (/pnl 엔드포인트는 유지) */
    val enabled: Boolean = true,
    /** PnL 로그 주기 (분) */
    val logIntervalMinutes: Long = 60,
    /** 총수익률 계산 기준이 되는 시작 자금. 미설정 시 수익률 없이 평가액만 리포트 */
    val initialCapital: java.math.BigDecimal? = null,
)
