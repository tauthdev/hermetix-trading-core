package com.tripleauth.hermetix.strategy

import com.tripleauth.hermetix.client.dto.CandleInterval
import java.time.Duration

/**
 * 전략의 실행 규격. 엔진은 이 정보만으로 데이터 준비와 호출 주기를 결정한다.
 */
data class StrategySpec(
    /** 전략 식별자. clientOrderId 프리픽스로 사용되므로 영문/숫자/하이픈만 권장 */
    val name: String,
    /** 감시 대상 심볼 목록 */
    val symbols: List<String>,
    /** 전략에 공급할 캔들 주기 */
    val candleInterval: CandleInterval = CandleInterval.DAY_1,
    /** 전략에 공급할 캔들 개수 */
    val candleLimit: Int = 30,
    /** 전략 호출 주기 ([TickTrigger.ON_TRADE] 에서는 스트림이 끊겼을 때의 안전망 주기) */
    val pollInterval: Duration = Duration.ofSeconds(60),
    /** true 면 정규장 시간에만 전략을 호출한다 */
    val regularHoursOnly: Boolean = true,
    /** 전략 호출을 촉발하는 것 — 주기 폴링 또는 체결가 스트림 */
    val trigger: TickTrigger = TickTrigger.POLL,
    /** [TickTrigger.ON_TRADE] 에서 연속 호출 사이의 최소 간격. REST 호출(캔들·계좌) 폭주를 막는다 */
    val minTickInterval: Duration = Duration.ofSeconds(1),
    /** true 면 심볼의 호가창 스트림을 구독해 [StrategyContext.orderBook] 으로 공급한다 (브로커가 ORDER_BOOK 채널을 선언한 경우만) */
    val orderBook: Boolean = false,
)
