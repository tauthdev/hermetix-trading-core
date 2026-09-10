package com.tripleauth.hermetix.broker

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class RateLimiterTest {

    private val sleeps = mutableListOf<Long>()
    private var now = 0L
    private fun limiter(interval: Long, retries: Int = 3) =
        RateLimiter(interval, retries, backoffMillis = { 1000L * it }, sleeper = { sleeps += it; now += it }, clock = { now })

    @Test
    fun `호출 간 최소 간격을 보장한다`() {
        val limiter = limiter(600)
        limiter.execute { now += 100; "a" } // 첫 호출은 대기 없음
        limiter.execute { "b" }              // 100ms 뒤 → 500ms 대기
        assertThat(sleeps).containsExactly(500L)
    }

    @Test
    fun `RateLimitError 는 백오프 후 재시도하고 소진되면 전파한다`() {
        val limiter = limiter(0, retries = 2)
        var calls = 0
        val result = limiter.execute {
            calls++
            if (calls < 3) throw RateLimitError(429, "EGW00201", "초당 거래건수 초과")
            "ok"
        }
        assertThat(result).isEqualTo("ok")
        assertThat(sleeps).containsExactly(1000L, 2000L) // 어댑터 백오프: 1s, 2s

        sleeps.clear()
        assertThatThrownBy { limiter.execute { throw RateLimitError(429, null, "still") } }
            .isInstanceOf(RateLimitError::class.java)
        assertThat(sleeps).hasSize(2)
    }

    @Test
    fun `서버 Retry-After 가 있으면 그 값을 (상한 내에서) 따른다`() {
        val limiter = limiter(0, retries = 1)
        var first = true
        limiter.execute { if (first) { first = false; throw RateLimitError(429, null, "rl", retryAfterSeconds = 3) }; "ok" }
        assertThat(sleeps).containsExactly(3000L)

        sleeps.clear()
        first = true
        limiter.execute { if (first) { first = false; throw RateLimitError(429, null, "rl", retryAfterSeconds = 3600) }; "ok" }
        assertThat(sleeps).containsExactly(RateLimiter.MAX_RETRY_AFTER_MILLIS)
    }

    @Test
    fun `다른 예외는 재시도하지 않는다`() {
        val limiter = limiter(0)
        var calls = 0
        assertThatThrownBy { limiter.execute { calls++; throw AuthError(401, null, "x") } }.isInstanceOf(AuthError::class.java)
        assertThat(calls).isEqualTo(1)
        assertThat(sleeps).isEmpty()
    }
}
