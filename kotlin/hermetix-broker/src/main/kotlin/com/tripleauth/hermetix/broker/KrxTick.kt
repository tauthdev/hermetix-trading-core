package com.tripleauth.hermetix.broker

import java.math.BigDecimal
import java.math.RoundingMode

/**
 * KRX 호가단위 보정 (2023-01 개정 기준).
 *
 * 지정가가 호가단위에 맞지 않으면 거래소가 주문을 거부하므로,
 * KRX 어댑터는 지정가를 가장 가까운 유효 호가로 내림 보정한다.
 */
object KrxTick {

    fun round(price: BigDecimal): BigDecimal {
        val tick = tickSize(price)
        return price.divide(tick, 0, RoundingMode.DOWN).multiply(tick)
    }

    fun tickSize(price: BigDecimal): BigDecimal = when {
        price < BigDecimal(2_000) -> BigDecimal(1)
        price < BigDecimal(5_000) -> BigDecimal(5)
        price < BigDecimal(20_000) -> BigDecimal(10)
        price < BigDecimal(50_000) -> BigDecimal(50)
        price < BigDecimal(200_000) -> BigDecimal(100)
        price < BigDecimal(500_000) -> BigDecimal(500)
        else -> BigDecimal(1_000)
    }
}
