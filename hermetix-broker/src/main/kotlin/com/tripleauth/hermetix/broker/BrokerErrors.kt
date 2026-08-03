package com.tripleauth.hermetix.broker

/**
 * 타입화된 브로커 에러 계층 (CCXT 에러 체계에 해당).
 *
 * 어댑터는 브로커별 에러 코드를 이 타입들로 매핑하고, 엔진은 타입별로 반응한다:
 * - [MarketClosedError] → 틱 조용히 스킵 (비상정지 카운트 제외)
 * - [RateLimitError] → 경고 후 다음 틱 대기 (비상정지 카운트 제외 — 어댑터가 이미 백오프 재시도한 뒤다)
 * - [InsufficientFundsError] → 해당 시그널만 스킵
 * - 그 외 → 틱 실패로 기록, 연속 시 비상정지
 */

/** 인증 실패 (키 오류, 토큰 발급 실패 등) */
class AuthError(httpStatus: Int, errorCode: String?, message: String?) :
    BrokerApiException(httpStatus, errorCode, message)

/** 레이트리밋 초과 — 어댑터의 자동 재시도가 모두 소진된 뒤에만 전파된다 */
class RateLimitError(httpStatus: Int, errorCode: String?, message: String?) :
    BrokerApiException(httpStatus, errorCode, message)

/** 장 마감/휴장으로 주문 불가 */
class MarketClosedError(httpStatus: Int, errorCode: String?, message: String?) :
    BrokerApiException(httpStatus, errorCode, message)

/** 주문 가능 금액/수량 부족 */
class InsufficientFundsError(httpStatus: Int, errorCode: String?, message: String?) :
    BrokerApiException(httpStatus, errorCode, message)

/** 주문 형식/값 오류 (가격 단위, 수량 등) */
class InvalidOrderError(httpStatus: Int, errorCode: String?, message: String?) :
    BrokerApiException(httpStatus, errorCode, message)

/** 존재하지 않는 주문 */
class OrderNotFoundError(errorCode: String?, message: String?) :
    BrokerApiException(404, errorCode, message)
