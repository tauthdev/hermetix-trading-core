package com.tripleauth.hermetix.strategy

/**
 * 전략 호출을 무엇이 촉발하는가.
 *
 * - [POLL]: [StrategySpec.pollInterval] 주기로만 호출한다 (기본, 모든 브로커)
 * - [ON_TRADE]: 브로커 체결가 스트림의 틱이 올 때마다 호출한다. 틱이 몰리면 하나로 합치고
 *   [StrategySpec.minTickInterval] 보다 촘촘히는 부르지 않는다. 폴링은 안전망으로 계속 돈다 —
 *   스트림이 끊겨도 전략은 pollInterval 주기로 계속 호출된다.
 *   브로커가 [com.tripleauth.hermetix.broker.StreamChannel.TRADES] 를 선언하지 않으면 경고 후 POLL 로 동작한다
 */
enum class TickTrigger { POLL, ON_TRADE }
