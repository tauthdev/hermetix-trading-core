package com.tripleauth.hermetix.client.toss

import com.tripleauth.hermetix.broker.TradingEnvironment
import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * 토스증권 Open API 설정. `hermetix.broker: toss` 로 활성화한다.
 *
 * **실전 전용** — 토스증권은 모의투자 샌드박스가 없다. 따라서 기본 환경이 LIVE 이고, 엔진은 `hermetix.live.enabled=true`
 * 와 주문 금액 상한(`hermetix.risk.*`) 없이는 기동하지 않는다. 허용 IP 는 WTS 설정 > Open API 에 미리 등록해야 한다(미등록 IP 는 403).
 */
@ConfigurationProperties(prefix = "hermetix.toss")
data class TossApiProperties(
    val environment: TradingEnvironment = TradingEnvironment.LIVE,
    val baseUrl: String = "https://openapi.tossinvest.com",
    /** WTS 설정 > Open API 에서 발급한 client_id (`c_…`) */
    val clientId: String = "",
    /** client_secret (`s_…`) — 발급 시 한 번만 표시 */
    val clientSecret: String = "",
    /** 계좌 순번(`accountSeq`). 비우면 `GET /api/v1/accounts` 의 첫 BROKERAGE 계좌를 쓴다 */
    val accountSeq: String = "",
    /** 호출 간 최소 간격(ms). 그룹별 한도가 다르다(자산 5/s, 주문정보 6/s, 계좌 1/s) — 기본 200ms(5/s) */
    val throttleMillis: Long = 200,
    /** 토큰 만료 전 미리 갱신할 여유 시간(초). client 당 유효 토큰이 1개라 다른 프로세스가 재발급하면 이 토큰은 무효가 된다 */
    val tokenRefreshMarginSeconds: Long = 60,
)
