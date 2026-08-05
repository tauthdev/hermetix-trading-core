package com.tripleauth.hermetix.client.kiwoom

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "hermetix.kiwoom")
data class KiwoomApiProperties(
    /** 모의투자 도메인. 실전은 https://api.kiwoom.com */
    val baseUrl: String = "https://mockapi.kiwoom.com",
    val appkey: String = "",
    val secretkey: String = "",
    /** 요청 간 최소 간격(ms) — 서버 레이트리밋 회피 */
    val throttleMillis: Long = 1100,
    /** 토큰 만료 전 미리 갱신할 여유 시간(초) */
    val tokenRefreshMarginSeconds: Long = 300,
)
