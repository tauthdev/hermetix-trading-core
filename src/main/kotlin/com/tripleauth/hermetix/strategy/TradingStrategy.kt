package com.tripleauth.hermetix.strategy

/**
 * 전략 작성자가 구현하는 유일한 인터페이스.
 *
 * ```kotlin
 * @Component
 * class MyStrategy : TradingStrategy {
 *
 *     override val spec = StrategySpec(
 *         name = "my-strategy",
 *         symbols = listOf("AAPL"),
 *         candleInterval = CandleInterval.DAY_1,
 *         candleLimit = 20,
 *     )
 *
 *     override fun decide(context: StrategyContext): List<Signal> {
 *         val candles = context.candles("AAPL")
 *         // ... 진입/청산 판단 ...
 *         return listOf(Signal.Buy(symbol = "AAPL", quantity = BigDecimal.ONE))
 *     }
 * }
 * ```
 *
 * 규약:
 * - [decide] 는 엔진이 [StrategySpec.pollInterval] 주기로 호출한다 (기본: 정규장 중에만)
 * - 반환한 [Signal] 목록은 엔진이 순서대로 실행한다. 할 일이 없으면 빈 리스트를 반환한다
 * - 전략 안에서 API 를 직접 호출하거나 스레드를 만들지 않는다
 * - 상태가 필요하면 전략 클래스 필드에 보관한다 (엔진은 전략 인스턴스를 재사용한다)
 */
interface TradingStrategy {

    val spec: StrategySpec

    fun decide(context: StrategyContext): List<Signal>
}
