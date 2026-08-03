package com.tripleauth.hermetix.engine

import com.tripleauth.hermetix.broker.BrokerClient
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * 비상정지 장치.
 *
 * 서버에 킬 스위치 API 가 아직 없으므로 클라이언트 측에서 동일 효과를 낸다:
 * 발동 시 모든 미체결 주문을 취소하고, 해제 전까지 신규 주문 실행을 차단한다.
 * 연속 실패가 임계치에 도달하면 자동 발동된다.
 */
class TradingGuard(
    private val brokerClient: BrokerClient,
    private val maxConsecutiveFailures: Int,
) {

    private val logger = KotlinLogging.logger { }

    private val halted = AtomicBoolean(false)
    private val consecutiveFailures = AtomicInteger(0)

    val isHalted: Boolean get() = halted.get()

    fun recordSuccess() {
        consecutiveFailures.set(0)
    }

    fun recordFailure(cause: Throwable) {
        val count = consecutiveFailures.incrementAndGet()
        logger.warn { "engine failure $count/$maxConsecutiveFailures - ${cause.message}" }
        if (count >= maxConsecutiveFailures && !halted.get()) {
            halt("연속 실패 ${count}회")
        }
    }

    fun halt(reason: String) {
        if (!halted.compareAndSet(false, true)) return
        logger.error { "TRADING HALTED / reason=$reason - cancelling all open orders" }

        runCatching {
            brokerClient.getOrders().orders
                .filter { it.status.isOpen }
                .forEach { order ->
                    runCatching { brokerClient.cancelOrder(order.orderId) }
                        .onSuccess { logger.info { "halt-cancel ok / orderId=${order.orderId}" } }
                        .onFailure { logger.error(it) { "halt-cancel failed / orderId=${order.orderId}" } }
                }
        }.onFailure { logger.error(it) { "halt: open order lookup failed" } }
    }

    fun resume() {
        consecutiveFailures.set(0)
        halted.set(false)
        logger.info { "trading resumed" }
    }
}
