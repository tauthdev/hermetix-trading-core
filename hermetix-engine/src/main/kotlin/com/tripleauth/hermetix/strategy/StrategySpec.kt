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
    /** 전략 호출 주기 */
    val pollInterval: Duration = Duration.ofSeconds(60),
    /** true 면 정규장 시간에만 전략을 호출한다 */
    val regularHoursOnly: Boolean = true,
)
