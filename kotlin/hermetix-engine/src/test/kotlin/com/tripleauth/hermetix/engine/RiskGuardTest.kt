package com.tripleauth.hermetix.engine

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class RiskGuardTest {

    @Test
    fun `상한이 없으면 아무것도 검사하지 않는다`() {
        val guard = RiskGuard()
        assertThat(guard.isActive).isFalse()
        assertThat(guard.tryReserve("AAPL", BigDecimal("1000"), null)).isNull()
    }

    @Test
    fun `1건 상한을 넘는 주문은 거부하고 누적하지 않는다`() {
        val guard = RiskGuard(maxOrderValue = BigDecimal("1000"))
        assertThat(guard.tryReserve("AAPL", BigDecimal("2"), BigDecimal("600"))).contains("1건 상한")
        assertThat(guard.tryReserve("AAPL", BigDecimal("1"), BigDecimal("600"))).isNull()
        assertThat(guard.dailyTotal()).isEqualByComparingTo("600")
    }

    @Test
    fun `일일 누적 상한은 매수·매도 합산이고 날짜가 바뀌면 초기화된다`() {
        var now = Instant.parse("2026-09-10T23:00:00Z")
        val clock = object : Clock() {
            override fun getZone() = ZoneOffset.UTC
            override fun withZone(zone: java.time.ZoneId) = this
            override fun instant() = now
        }
        val guard = RiskGuard(maxDailyOrderValue = BigDecimal("1000"), clock = clock)

        assertThat(guard.tryReserve("AAPL", BigDecimal("2"), BigDecimal("300"))).isNull()   // 600
        assertThat(guard.tryReserve("AAPL", BigDecimal("1"), BigDecimal("300"))).isNull()   // 900
        assertThat(guard.tryReserve("AAPL", BigDecimal("1"), BigDecimal("300"))).contains("일일 누적") // 1200 > 1000
        assertThat(guard.dailyTotal()).isEqualByComparingTo("900")

        now = Instant.parse("2026-09-11T01:00:00Z") // UTC 자정 경과
        assertThat(guard.tryReserve("AAPL", BigDecimal("1"), BigDecimal("300"))).isNull()
        assertThat(guard.dailyTotal()).isEqualByComparingTo("300")
    }

    @Test
    fun `가격을 모르면 상한이 설정된 경우 거부한다`() {
        val guard = RiskGuard(maxOrderValue = BigDecimal("1000"))
        assertThat(guard.tryReserve("AAPL", BigDecimal("1"), null)).contains("가격을 알 수 없어")
        assertThat(guard.tryReserve("AAPL", BigDecimal("1"), BigDecimal.ZERO)).contains("가격을 알 수 없어")
    }
}
