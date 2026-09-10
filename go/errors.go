package hermetix

import "fmt"

// BrokerAPIError - 타입화된 브로커 에러 계층의 기반. 엔진의 타입별 반응:
//   - *MarketClosedError  -> 틱 조용히 스킵 (비상정지 카운트 제외)
//   - *RateLimitError     -> 어댑터 백오프 소진 후 전파 시 틱 스킵 (카운트 제외)
//   - *InsufficientFundsError -> 해당 시그널만 스킵
//   - 그 외               -> 틱 실패로 기록, 연속 시 비상정지
type BrokerAPIError struct {
	HTTPStatus int
	Code       string
	Message    string
}

func (e *BrokerAPIError) Error() string {
	return fmt.Sprintf("[%d/%s] %s", e.HTTPStatus, e.Code, e.Message)
}

type AuthError struct{ BrokerAPIError }

// RateLimitError - 레이트리밋 초과. 어댑터의 자동 재시도(rateLimiter)가 소진된 뒤에만 전파된다.
// RetryAfterSeconds 는 서버 Retry-After (0 이면 없음 → 어댑터 기본 백오프).
type RateLimitError struct {
	BrokerAPIError
	RetryAfterSeconds float64
}
type MarketClosedError struct{ BrokerAPIError }
type InsufficientFundsError struct{ BrokerAPIError }
type InvalidOrderError struct{ BrokerAPIError }
type OrderNotFoundError struct{ BrokerAPIError }

func newAuthError(status int, code, msg string) *AuthError {
	return &AuthError{BrokerAPIError{status, code, msg}}
}

// newRateLimitErrorWithRetryAfter - 서버 Retry-After(초)를 담는다. 0 이면 어댑터 기본 백오프.
func newRateLimitErrorWithRetryAfter(status int, code, msg string, retryAfterSeconds float64) *RateLimitError {
	return &RateLimitError{BrokerAPIError{status, code, msg}, retryAfterSeconds}
}

func newRateLimitError(status int, code, msg string) *RateLimitError {
	return &RateLimitError{BrokerAPIError{status, code, msg}, 0}
}

func newMarketClosedError(status int, code, msg string) *MarketClosedError {
	return &MarketClosedError{BrokerAPIError{status, code, msg}}
}

func newOrderNotFoundError(code, msg string) *OrderNotFoundError {
	return &OrderNotFoundError{BrokerAPIError{404, code, msg}}
}
