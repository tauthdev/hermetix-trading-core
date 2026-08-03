package com.tripleauth.nexttrading.autoconfigure

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "next.engine")
data class NextEngineProperties(
    /** false 면 전략 엔진을 기동하지 않는다 (API 클라이언트만 사용) */
    val enabled: Boolean = true,
    /** 연속 실패가 이 횟수에 도달하면 비상정지(미체결 전량 취소 + 신규 주문 차단) */
    val maxConsecutiveFailures: Int = 5,
)
