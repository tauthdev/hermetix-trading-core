package com.tripleauth.hermetix.broker

import com.tripleauth.hermetix.client.dto.CalendarResponse
import com.tripleauth.hermetix.client.dto.MarketDay
import com.tripleauth.hermetix.client.dto.MarketSessions
import com.tripleauth.hermetix.client.dto.SessionHours
import java.time.DayOfWeek
import java.time.LocalDate
import java.time.ZoneId

/**
 * KRX 정규장 합성 캘린더 (KIS/키움 어댑터 공용).
 *
 * 공휴일은 반영하지 못한다 — 휴장일에는 개장일로 보이지만 주문 시 각 서버가
 * 거부(장종료)하므로 안전에는 문제가 없다. 엔진이 헛틱을 도는 비용만 있다.
 */
object KrxCalendar {

    val KST: ZoneId = ZoneId.of("Asia/Seoul")

    fun synthesize(days: Long = 31): CalendarResponse {
        val today = LocalDate.now(KST)
        return CalendarResponse(
            (0 until days).map { offset ->
                val date = today.plusDays(offset)
                val weekday = date.dayOfWeek != DayOfWeek.SATURDAY && date.dayOfWeek != DayOfWeek.SUNDAY
                MarketDay(
                    date = date.toString(),
                    open = weekday,
                    sessions = if (weekday) MarketSessions(
                        preMarket = null,
                        regular = SessionHours(start = "09:00", end = "15:30"),
                        afterHours = null,
                    ) else null,
                    timezone = "Asia/Seoul",
                    holiday = null,
                )
            },
        )
    }
}
