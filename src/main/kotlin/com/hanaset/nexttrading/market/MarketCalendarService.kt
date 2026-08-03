package com.hanaset.nexttrading.market

import com.hanaset.nexttrading.client.NextApiClient
import com.hanaset.nexttrading.client.dto.MarketDay
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime

/**
 * 미국 주식시장 개장 여부 판단. `/v1/market/calendar` 응답을 캐시해 사용한다.
 */
class MarketCalendarService(
    private val nextApiClient: NextApiClient,
) {

    private val logger = KotlinLogging.logger { }

    @Volatile
    private var cache: Pair<Instant, Map<LocalDate, MarketDay>>? = null

    fun isRegularOpen(now: ZonedDateTime = ZonedDateTime.now()): Boolean {
        val day = todayEntry(now) ?: return false
        if (!day.open) return false

        val regular = day.sessions?.regular ?: return false
        val zone = ZoneId.of(day.timezone)
        val local = now.withZoneSameInstant(zone)

        val start = LocalTime.parse(regular.start)
        val end = LocalTime.parse(regular.end)

        return !local.toLocalTime().isBefore(start) && local.toLocalTime().isBefore(end)
    }

    private fun todayEntry(now: ZonedDateTime): MarketDay? {
        val days = calendar()
        // 캘린더의 타임존(뉴욕) 기준 날짜로 조회한다
        val zone = days.values.firstOrNull()?.timezone?.let { ZoneId.of(it) } ?: ZoneId.of("America/New_York")
        val today = now.withZoneSameInstant(zone).toLocalDate()
        return days[today]
    }

    private fun calendar(): Map<LocalDate, MarketDay> {
        val current = cache
        if (current != null && current.first.isAfter(Instant.now().minus(CACHE_TTL))) {
            return current.second
        }

        val response = nextApiClient.getCalendar()
        val byDate = response.calendar.associateBy { LocalDate.parse(it.date) }
        cache = Instant.now() to byDate

        logger.info { "market calendar refreshed / days=${byDate.size}" }

        return byDate
    }

    companion object {
        private val CACHE_TTL: Duration = Duration.ofHours(6)
    }
}
