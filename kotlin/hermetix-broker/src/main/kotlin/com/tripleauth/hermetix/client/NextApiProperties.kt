package com.tripleauth.hermetix.client

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "hermetix.next")
data class NextApiProperties(
    val baseUrl: String = "https://openapi.nextsecurities.dev",
    val clientId: String = "",
    val clientSecret: String = "",
    /** X-Next-Account-Id 헤더 값 (v1.3). 계좌·자산·주문 API 필수 — 비우면 401, 토큰의 계좌와 다르면 403 account-mismatch. */
    val accountId: String = "",
    /** 토큰 만료 전 미리 갱신할 여유 시간(초) */
    val tokenRefreshMarginSeconds: Long = 60,
)
