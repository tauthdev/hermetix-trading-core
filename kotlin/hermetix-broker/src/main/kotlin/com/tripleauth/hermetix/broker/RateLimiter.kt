package com.tripleauth.hermetix.broker

import io.github.oshai.kotlinlogging.KotlinLogging

/**
 * 어댑터 공용 레이트리밋 부품 — 호출 간 최소 간격(쓰로틀) + [RateLimitError] 백오프 재시도.
 *
 * - [minIntervalMillis]: 호출 간 최소 간격. 0 이면 쓰로틀 없음 (넥스트처럼 초당 한도가 넉넉한 브로커)
 * - [maxRetries]: [RateLimitError] 발생 시 재시도 횟수. 소진되면 마지막 예외를 그대로 던진다 —
 *   엔진은 그때서야 `RateLimitError` 를 보고 틱을 건너뛴다 (어댑터가 이미 백오프한 뒤라는 계약)
 * - 대기 시간: 서버가 `Retry-After` 를 줬으면([RateLimitError.retryAfterSeconds]) 그 값, 아니면 [backoffMillis]
 */
class RateLimiter(
    private val minIntervalMillis: Long,
    private val maxRetries: Int = 3,
    private val backoffMillis: (attempt: Int) -> Long = { attempt -> 1000L * attempt },
    private val sleeper: (Long) -> Unit = Thread::sleep,
    private val clock: () -> Long = System::currentTimeMillis,
) {

    private val logger = KotlinLogging.logger { }

    private val lock = Any()
    private var lastCallAt: Long? = null

    /** 쓰로틀 후 [block] 실행. [RateLimitError] 면 백오프 후 재시도 */
    fun <T> execute(label: String = "", block: () -> T): T {
        var attempt = 0
        while (true) {
            throttle()
            try {
                return block()
            } catch (e: RateLimitError) {
                if (attempt >= maxRetries) throw e
                attempt++
                val wait = e.retryAfterSeconds?.let { it * 1000 }?.coerceAtMost(MAX_RETRY_AFTER_MILLIS) ?: backoffMillis(attempt)
                logger.warn { "rate limit${if (label.isBlank()) "" else "($label)"} - retry $attempt/$maxRetries after ${wait}ms" }
                sleeper(wait)
            }
        }
    }

    /** 호출 간 최소 간격 보장 — 토큰 발급처럼 [execute] 밖에서 호출하는 경로용 */
    fun throttle() {
        if (minIntervalMillis <= 0) return
        synchronized(lock) {
            val last = lastCallAt
            if (last != null) {
                val wait = last + minIntervalMillis - clock()
                if (wait > 0) sleeper(wait)
            }
            lastCallAt = clock()
        }
    }

    companion object {
        /** 서버 Retry-After 를 그대로 믿되 상한을 둔다 — 틱 하나가 무한히 막히지 않도록 */
        const val MAX_RETRY_AFTER_MILLIS = 30_000L
    }
}
