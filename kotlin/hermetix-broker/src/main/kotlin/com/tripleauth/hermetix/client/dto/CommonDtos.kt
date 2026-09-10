package com.tripleauth.hermetix.client.dto

import com.fasterxml.jackson.annotation.JsonProperty

data class TokenResponse(
    @JsonProperty("access_token") val accessToken: String,
    @JsonProperty("token_type") val tokenType: String,
    @JsonProperty("expires_in") val expiresIn: Long,
)

/** 토큰 발급 API(400/401) 전용 OAuth 표준 에러 형식 — 다른 API 의 [ApiErrorEnvelope] 와 다르다 */
data class OAuthErrorResponse(
    @JsonProperty("error") val error: String?,
    @JsonProperty("error_description") val errorDescription: String?,
)

data class ApiErrorEnvelope(
    val error: ApiError,
)

data class ApiError(
    val type: String?,
    val code: String?,
    val message: String?,
    val param: String?,
    val requestId: String?,
    val docUrl: String?,
)
