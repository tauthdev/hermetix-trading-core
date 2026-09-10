/**
 * 타입화된 브로커 에러 계층. 엔진의 타입별 반응:
 * - MarketClosedError  -> 틱 조용히 스킵 (비상정지 카운트 제외)
 * - RateLimitError     -> 어댑터 백오프 소진 후 전파 시 틱 스킵 (카운트 제외)
 * - InsufficientFundsError -> 해당 시그널만 스킵
 * - 그 외              -> 틱 실패로 기록, 연속 시 비상정지
 */

export class BrokerApiError extends Error {
  constructor(
    public readonly httpStatus: number,
    public readonly errorCode: string | null,
    message: string,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

export class AuthError extends BrokerApiError {}
/** 레이트리밋 초과 — 어댑터의 자동 재시도(RateLimiter)가 소진된 뒤에만 전파된다. retryAfterSeconds 는 서버 Retry-After */
export class RateLimitError extends BrokerApiError {
  constructor(httpStatus: number, errorCode: string | null, message: string, readonly retryAfterSeconds: number | null = null) {
    super(httpStatus, errorCode, message);
  }
}
export class MarketClosedError extends BrokerApiError {}
export class InsufficientFundsError extends BrokerApiError {}
export class InvalidOrderError extends BrokerApiError {}
export class OrderNotFoundError extends BrokerApiError {
  constructor(errorCode: string | null, message: string) {
    super(404, errorCode, message);
  }
}
