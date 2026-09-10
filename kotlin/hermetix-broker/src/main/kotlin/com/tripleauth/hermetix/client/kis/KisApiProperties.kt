package com.tripleauth.hermetix.client.kis

import com.tripleauth.hermetix.broker.TradingEnvironment
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "hermetix.kis")
data class KisApiProperties(
    /** 거래 환경. 호스트와 TR ID 프리픽스(모의 V / 실전 T)가 이에 따라 결정된다 */
    val environment: TradingEnvironment = TradingEnvironment.PAPER,
    /** 비우면 환경에 따라 결정 — 모의 https://openapivts.koreainvestment.com:29443, 실전 https://openapi.koreainvestment.com:9443 */
    val baseUrl: String = "",
    val appkey: String = "",
    val appsecret: String = "",
    /** 종합계좌번호 (8자리) */
    val cano: String = "",
    /** 계좌상품코드 (2자리, 보통 01) */
    val acntPrdtCd: String = "01",
    /** 고객타입 (P: 개인) */
    val custtype: String = "P",
    /** 초당 요청 제한 회피용 최소 호출 간격(ms). 0 이면 환경에 따라 자동 — 모의 600(초당 2건), 실전 100(초당 20건 한도의 절반) */
    val throttleMillis: Long = 0,
    /** 토큰 만료 전 미리 갱신할 여유 시간(초) */
    val tokenRefreshMarginSeconds: Long = 300,
) {
    val isLive: Boolean get() = environment == TradingEnvironment.LIVE

    fun resolvedBaseUrl(): String = baseUrl.ifBlank {
        if (isLive) "https://openapi.koreainvestment.com:9443" else "https://openapivts.koreainvestment.com:29443"
    }

    fun resolvedThrottleMillis(): Long = if (throttleMillis > 0) throttleMillis else if (isLive) 100 else 600

    /** 계좌 TR ID — 모의는 V, 실전은 T 프리픽스 (예: `tr("TTC0802U")` → VTTC0802U / TTTC0802U) */
    fun tr(suffix: String): String = (if (isLive) "T" else "V") + suffix
}
