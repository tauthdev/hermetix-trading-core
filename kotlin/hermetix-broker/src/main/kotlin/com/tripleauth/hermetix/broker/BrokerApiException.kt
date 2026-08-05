package com.tripleauth.hermetix.broker

/**
 * 브로커 API 호출 실패의 공통 예외.
 *
 * 어댑터는 이 예외(또는 하위 타입)로 실패를 표현한다. 엔진은 타입에 관계없이
 * 틱 단위로 실패를 격리하고 TradingGuard 카운트에 반영한다.
 */
open class BrokerApiException(
    val httpStatus: Int,
    val errorCode: String?,
    override val message: String?,
    cause: Throwable? = null,
) : RuntimeException(message, cause)
