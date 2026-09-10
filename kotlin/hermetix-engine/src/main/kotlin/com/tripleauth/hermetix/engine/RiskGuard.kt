package com.tripleauth.hermetix.engine

import io.github.oshai.kotlinlogging.KotlinLogging
import java.math.BigDecimal
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneOffset

/**
 * 주문 금액 상한 — 실전투자에서 봇 폭주로 인한 손실 규모를 제한한다 (모의투자에서도 설정하면 적용).
 *
 * - `maxOrderValue`: 주문 1건의 추정 금액(수량 × 지정가 또는 현재가) 상한
 * - `maxDailyOrderValue`: 하루(UTC 기준) 누적 주문 금액 상한. 매수·매도 모두 누적한다
 *
 * 추정 금액을 계산할 수 없으면(현재가 없음) 상한이 설정된 경우 주문을 거부한다 — 알 수 없는 금액을 통과시키지 않는다.
 * 누적치는 메모리에만 있다 (재시작 시 초기화). 상한이 둘 다 없으면 아무것도 검사하지 않는다.
 */
class RiskGuard(
    private val maxOrderValue: BigDecimal? = null,
    private val maxDailyOrderValue: BigDecimal? = null,
    private val clock: Clock = Clock.systemUTC(),
) {

    private val logger = KotlinLogging.logger { }

    private val lock = Any()
    private var day: LocalDate = LocalDate.now(clock.withZone(ZoneOffset.UTC))
    private var dailyTotal: BigDecimal = BigDecimal.ZERO

    val isActive: Boolean get() = maxOrderValue != null || maxDailyOrderValue != null

    /**
     * 주문을 허용하면 일일 누적에 반영하고 null 을, 거부하면 사유를 돌려준다.
     * 누적은 주문 제출 전에 잡는다 — 제출이 실패해도 되돌리지 않는다 (보수적).
     */
    fun tryReserve(symbol: String, quantity: BigDecimal, price: BigDecimal?): String? {
        if (!isActive) return null
        if (price == null || price <= BigDecimal.ZERO) {
            return "가격을 알 수 없어 주문 금액 상한을 검증할 수 없습니다 (symbol=$symbol)"
        }

        val value = quantity.multiply(price)
        if (maxOrderValue != null && value > maxOrderValue) {
            return "주문 금액 $value 이(가) 1건 상한 $maxOrderValue 을(를) 초과합니다 (symbol=$symbol)"
        }

        synchronized(lock) {
            rollDay()
            if (maxDailyOrderValue != null && dailyTotal + value > maxDailyOrderValue) {
                return "일일 누적 주문 금액 ${dailyTotal + value} 이(가) 상한 $maxDailyOrderValue 을(를) 초과합니다 (오늘 누적=$dailyTotal)"
            }
            dailyTotal += value
            logger.debug { "risk reserve / $symbol value=$value dailyTotal=$dailyTotal" }
        }
        return null
    }

    fun dailyTotal(): BigDecimal = synchronized(lock) { rollDay(); dailyTotal }

    private fun rollDay() {
        val today = LocalDate.now(clock.withZone(ZoneOffset.UTC))
        if (today != day) {
            day = today
            dailyTotal = BigDecimal.ZERO
        }
    }
}
