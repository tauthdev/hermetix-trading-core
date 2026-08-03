package com.hanaset.nexttrading.client

import com.fasterxml.jackson.databind.ObjectMapper
import com.hanaset.nexttrading.client.dto.ApiErrorEnvelope
import com.hanaset.nexttrading.client.dto.TokenResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.http.MediaType
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestClient
import java.time.Instant

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
            .contentType(MediaType.APPLICATION_FORM_URLENCODED)
            .body(form)
            .exchange { _, res ->
                val body = res.body.readAllBytes()
                if (res.statusCode.is2xxSuccessful) {
                    objectMapper.readValue(body, TokenResponse::class.java)
                } else {
                    val error = runCatching { objectMapper.readValue(body, ApiErrorEnvelope::class.java).error }.getOrNull()
                    throw NextApiException(res.statusCode.value(), error)
                }
            }!!

        cached = CachedToken(
            accessToken = response.accessToken,
            expiresAt = Instant.now().plusSeconds(response.expiresIn),
        )

        logger.info { "token refreshed / expiresIn=${response.expiresIn}s" }

        return response.accessToken
    }

    private data class CachedToken(
        val accessToken: String,
        val expiresAt: Instant,
    )
}
