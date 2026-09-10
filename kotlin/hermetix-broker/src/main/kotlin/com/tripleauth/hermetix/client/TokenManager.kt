package com.tripleauth.hermetix.client

import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.AuthError
import com.tripleauth.hermetix.client.dto.ApiErrorEnvelope
import com.tripleauth.hermetix.client.dto.OAuthErrorResponse
import com.tripleauth.hermetix.client.dto.TokenResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.http.MediaType
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestClient
import java.time.Instant

/**
 * 넥스트증권 액세스 토큰 캐시/갱신 (OAuth 2.0 Client Credentials).
 *
 * 공개 스펙 v1.3: 토큰 유효기간 12시간(expires_in=43200), refresh 토큰 없음.
 * 토큰 발급 API 만 에러 형식이 다르다 — 400/401 은 OAuth 표준 `{error, error_description}`,
 * 429/5xx 는 플랫폼 에러 엔벨로프. 둘 다 [AuthError] 로 변환한다.
 */
class TokenManager(
    private val properties: NextApiProperties,
    private val objectMapper: ObjectMapper,
) {

    private val logger = KotlinLogging.logger { }

    private val restClient = RestClient.builder()
        .baseUrl(properties.baseUrl)
        .build()

    @Volatile
    private var cached: CachedToken? = null

    fun getToken(): String {
        val current = cached
        if (current != null && current.expiresAt.isAfter(Instant.now().plusSeconds(properties.tokenRefreshMarginSeconds))) {
            return current.accessToken
        }
        return refresh()
    }

    /** 401 응답을 받았을 때 강제로 새 토큰을 발급받는다. */
    fun invalidate() {
        cached = null
    }

    @Synchronized
    private fun refresh(): String {
        val current = cached
        if (current != null && current.expiresAt.isAfter(Instant.now().plusSeconds(properties.tokenRefreshMarginSeconds))) {
            return current.accessToken
        }

        val form = LinkedMultiValueMap<String, String>().apply {
            add("grant_type", "client_credentials")
            add("client_id", properties.clientId)
            add("client_secret", properties.clientSecret)
        }

        val response = restClient.post()
            .uri("/v1/oauth/token")
            .header(NextApiClient.REQUEST_ID_HEADER, NextApiClient.newRequestId())
            .contentType(MediaType.APPLICATION_FORM_URLENCODED)
            .body(form)
            .exchange { _, res ->
                val body = res.body.readAllBytes()
                if (res.statusCode.is2xxSuccessful) {
                    objectMapper.readValue(body, TokenResponse::class.java)
                } else {
                    throw parseTokenError(res.statusCode.value(), body)
                }
            }!!

        cached = CachedToken(
            accessToken = response.accessToken,
            expiresAt = Instant.now().plusSeconds(response.expiresIn),
        )

        logger.info { "token refreshed / expiresIn=${response.expiresIn}s" }

        return response.accessToken
    }

    private fun parseTokenError(status: Int, body: ByteArray): AuthError {
        // OAuth 표준 형식 (400/401): {"error":"invalid_client","error_description":"..."}
        runCatching { objectMapper.readValue(body, OAuthErrorResponse::class.java) }
            .getOrNull()
            ?.takeIf { it.error != null }
            ?.let { return AuthError(status, it.error, "Next 토큰 발급 실패(${it.error}): ${it.errorDescription ?: ""}") }

        // 플랫폼 엔벨로프 (429/5xx): {"error":{"code":..,"message":..}}
        val error = runCatching { objectMapper.readValue(body, ApiErrorEnvelope::class.java).error }.getOrNull()
        return AuthError(status, error?.code, "Next 토큰 발급 실패(${error?.code}): ${error?.message ?: ""}")
    }

    private data class CachedToken(
        val accessToken: String,
        val expiresAt: Instant,
    )
}
