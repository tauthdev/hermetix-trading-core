"""타입화된 브로커 에러 계층 (Kotlin hermetix-broker 의 BrokerErrors 와 동일 의미).

어댑터는 브로커별 에러 코드를 이 타입들로 매핑하고, 엔진은 타입별로 반응한다:
- MarketClosedError  -> 틱 조용히 스킵 (비상정지 카운트 제외)
- RateLimitError     -> 어댑터가 백오프 재시도 후에도 남으면 틱 스킵 (카운트 제외)
- InsufficientFundsError -> 해당 시그널만 스킵
- 그 외              -> 틱 실패로 기록, 연속 시 비상정지
"""


class BrokerApiError(Exception):
    def __init__(self, http_status: int, error_code: str | None, message: str | None):
        super().__init__(message or "")
        self.http_status = http_status
        self.error_code = error_code
        self.message = message


class AuthError(BrokerApiError):
    """인증 실패 (키 오류, 토큰 발급 실패 등)"""


class RateLimitError(BrokerApiError):
    """레이트리밋 초과 - 어댑터의 자동 재시도(RateLimiter)가 소진된 뒤에만 전파된다.
    retry_after_seconds 는 서버가 Retry-After 로 알려준 대기 시간 (없으면 None → 어댑터 기본 백오프)"""

    def __init__(self, http_status: int, error_code: str | None, message: str | None,
                 retry_after_seconds: float | None = None):
        super().__init__(http_status, error_code, message)
        self.retry_after_seconds = retry_after_seconds


class MarketClosedError(BrokerApiError):
    """장 마감/휴장으로 주문 불가"""


class InsufficientFundsError(BrokerApiError):
    """주문 가능 금액/수량 부족"""


class InvalidOrderError(BrokerApiError):
    """주문 형식/값 오류 (가격 단위, 수량 등)"""


class OrderNotFoundError(BrokerApiError):
    """존재하지 않는 주문"""

    def __init__(self, error_code: str | None, message: str | None):
        super().__init__(404, error_code, message)
