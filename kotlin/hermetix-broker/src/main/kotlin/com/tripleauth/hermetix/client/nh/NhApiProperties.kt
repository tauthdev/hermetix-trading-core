package com.tripleauth.hermetix.client.nh

import com.tripleauth.hermetix.broker.TradingEnvironment
import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * NH투자증권 NH PLUG(나무 PLUG) REST OpenAPI 설정. `hermetix.broker: nh` 로 활성화한다.
 *
 * 모의/운영은 **호스트로만** 구분된다 (모의 `moapi.nhplug.com`, 운영 `api.nhplug.com`). 토큰 발급은 운영 호스트에서만 되고
 * 발급된 토큰은 양쪽에 쓸 수 있다. 계좌는 환경과 `acct_type` 이 맞아야 한다 (모의 `03`, 운영 `01`).
 */
@ConfigurationProperties(prefix = "hermetix.nh")
data class NhApiProperties(
    val environment: TradingEnvironment = TradingEnvironment.PAPER,
    /** 비우면 환경에 따라 결정 — 모의 https://moapi.nhplug.com:8443, 운영 https://api.nhplug.com:8443 */
    val baseUrl: String = "",
    /** 토큰 발급 호스트 — 운영 전용 (`/oauth2/token` 은 moapi 에 없다) */
    val authUrl: String = "https://api.nhplug.com:8443",
    val appKey: String = "",
    val appSecret: String = "",
    /** 계좌번호(11자리). 비우면 `/n2/acctinfo` 에서 환경에 맞는 `acct_type` 의 첫 계좌를 고른다 */
    val accountNo: String = "",
    /** 시세·잔고 시장 구분: `KRX` | `NXT` | `UNT`(통합) */
    val marketCd: String = "KRX",
    /** 주문 시장: `KRX` | `NXT` | `SOR` */
    val orderMarketCd: String = "KRX",
    /** 호출 간 최소 간격(ms). NH PLUG 는 초당 5회 한도 — 기본 250ms(초당 4회) */
    val throttleMillis: Long = 250,
    /** 토큰 만료 전 미리 갱신할 여유 시간(초) */
    val tokenRefreshMarginSeconds: Long = 300,
) {
    val isLive: Boolean get() = environment == TradingEnvironment.LIVE

    fun resolvedBaseUrl(): String = baseUrl.ifBlank { if (isLive) "https://api.nhplug.com:8443" else "https://moapi.nhplug.com:8443" }

    /** 환경에 맞는 계좌 구분 코드 — 모의 03, 운영 01 */
    fun expectedAcctType(): String = if (isLive) "01" else "03"
}
