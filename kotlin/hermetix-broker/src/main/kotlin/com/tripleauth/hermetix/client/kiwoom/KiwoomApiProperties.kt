package com.tripleauth.hermetix.client.kiwoom

import com.tripleauth.hermetix.broker.TradingEnvironment
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "hermetix.kiwoom")
data class KiwoomApiProperties(
    /** 거래 환경. 호스트가 이에 따라 결정된다 (TR ID 는 모의/실전 공통) */
    val environment: TradingEnvironment = TradingEnvironment.PAPER,
    /** 비우면 환경에 따라 결정 — 모의 https://mockapi.kiwoom.com, 실전 https://api.kiwoom.com */
    val baseUrl: String = "",
    val appkey: String = "",
    val secretkey: String = "",
    /** 요청 간 최소 간격(ms) — 서버 레이트리밋 회피 (TR 당 초당 1회) */
    val throttleMillis: Long = 1100,
    /** 토큰 만료 전 미리 갱신할 여유 시간(초) */
    val tokenRefreshMarginSeconds: Long = 300,
) {
    val isLive: Boolean get() = environment == TradingEnvironment.LIVE

    fun resolvedBaseUrl(): String = baseUrl.ifBlank { if (isLive) "https://api.kiwoom.com" else "https://mockapi.kiwoom.com" }
}
