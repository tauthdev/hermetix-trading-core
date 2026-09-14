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
    /**
     * 실시간 웹소켓 주소 (AsyncAPI 1.2.2, 실측 전). 모의투자 서버가 없어 실전 하나뿐이다.
     * 한도: 계정당 동시 연결 2개(3번째가 오면 가장 오래된 연결을 서버가 끊는다), 연결당 구독 100개(채널×종목 합, personal:order 계좌 포함),
     * 구독 선언 5회/초. 180초 동안 클라이언트가 아무것도 보내지 않으면 서버가 끊으므로 60초마다 텍스트 `PING` 을 보낸다.
     * 토큰은 핸드셰이크에서만 검사되고 연결 중 만료돼도 끊기지 않는다 — 재접속 때 REST 어댑터가 캐시한 토큰을 그대로 쓴다(재발급하면 이전 토큰이 무효).
     */
    val wsUrl: String = "wss://openapi-ws.tossinvest.com/ws/v1",
)
