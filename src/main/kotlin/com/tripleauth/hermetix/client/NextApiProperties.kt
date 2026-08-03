package com.tripleauth.hermetix.client

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "hermetix.next")
data class NextApiProperties(
    val baseUrl: String = "https://openapi.nextsecurities.dev",
    val clientId: String = "",
    val clientSecret: String = "",
    /** X-Nextsecurities-Account 헤더 값. 비우면 토큰의 기본 계좌를 사용하지 못하는 API 에서 실패한다. */
    val accountId: String = "",
    /** 토큰 만료 전 미리 갱신할 여유 시간(초) */
    val tokenRefreshMarginSeconds: Long = 60,
)
