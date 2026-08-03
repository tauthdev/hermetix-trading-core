package com.tripleauth.hermetix.client

import com.tripleauth.hermetix.client.dto.ApiError

class NextApiException(
    val httpStatus: Int,
    val error: ApiError?,
    cause: Throwable? = null,
) : RuntimeException(
    "Next API error (HTTP $httpStatus) code=${error?.code} message=${error?.message} requestId=${error?.requestId}",
    cause,
) {
    val code: String? get() = error?.code

    val isAuthError: Boolean
        get() = httpStatus == 401 || error?.type == "authentication"

    val isRetryable: Boolean
        get() = httpStatus >= 500 || httpStatus == 429
}
