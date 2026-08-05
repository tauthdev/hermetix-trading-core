package com.tripleauth.hermetix.autoconfigure

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "hermetix.engine")
data class NextEngineProperties(
    /** false 면 전략 엔진을 기동하지 않는다 (API 클라이언트만 사용) */
    val enabled: Boolean = true,
    /** 연속 실패가 이 횟수에 도달하면 비상정지(미체결 전량 취소 + 신규 주문 차단) */
    val maxConsecutiveFailures: Int = 5,
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
