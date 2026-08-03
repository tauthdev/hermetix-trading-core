package com.hanaset.nexttrading.client.dto

import com.fasterxml.jackson.annotation.JsonProperty

data class TokenResponse(
    @JsonProperty("access_token") val accessToken: String,
    @JsonProperty("token_type") val tokenType: String,
    @JsonProperty("expires_in") val expiresIn: Long,
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
