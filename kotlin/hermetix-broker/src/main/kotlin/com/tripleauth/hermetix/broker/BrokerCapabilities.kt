package com.tripleauth.hermetix.broker

import com.tripleauth.hermetix.client.dto.CandleInterval

/**
 * 브로커가 지원하는 기능의 코드 선언.
 *
 * 엔진은 이 선언을 보고 스스로 적응한다:
 * - 기동 시 전략의 candleInterval 이 미지원이면 스케줄하지 않고 명확히 알린다 (fail-fast)
 * - clientOrderId 미지원 브로커에는 아예 보내지 않는다
 * - nativeBracket 지원 브로커가 생기면 소프트웨어 브라켓 대신 네이티브 주문으로 전환한다
 *
 * 새 어댑터는 실측으로 확인한 것만 true 로 선언한다 — 낙관 선언 금지.
 */
data class BrokerCapabilities(
    /** 어댑터 식별자 — `hermetix.broker` 설정값과 동일 */
    val brokerId: String,
    /** 거래 시장 (예: "US", "KRX") */
    val market: String,
    /** 표시 통화 */
    val currency: String,
    /** 지원하는 캔들 주기 */
    val candleIntervals: Set<CandleInterval>,
    /** 주문 멱등 키(clientOrderId) 지원 여부 */
    val clientOrderId: Boolean,
    /** 서버 네이티브 브라켓(진입+익절+손절) 주문 지원 여부 */
    val nativeBracket: Boolean,
    /** 소수점 주식 거래 지원 여부 */
    val fractionalShares: Boolean,
    /**
     * 서버가 미체결 주문 목록 조회를 제공하는지.
     * false 면 어댑터가 메모리에서 자체 추적한다 — 앱 재시작 시 추적이 끊기므로
     * 재시작 후 서버에 남아있을 수 있는 미체결 주문은 보이지 않는다.
     */
    val serverOpenOrders: Boolean = true,
)
