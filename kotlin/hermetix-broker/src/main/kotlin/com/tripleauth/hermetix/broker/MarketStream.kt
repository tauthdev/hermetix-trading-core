package com.tripleauth.hermetix.broker

import com.tripleauth.hermetix.client.dto.Quote
import java.math.BigDecimal
import java.time.Instant

/**
 * 브로커가 제공하는 실시간 스트림 채널. [BrokerCapabilities.streams] 로 선언한다.
 */
enum class StreamChannel {
    /** 체결가 스트림 — 체결이 일어날 때마다 [TradeTick] 을 밀어준다 */
    TRADES,
}

/**
 * 체결 1건. 브로커 프레임을 공통 모델로 정규화한 것.
 *
 * - [symbol] 은 구독 요청 표기 그대로 돌려준다 (`KRX:005930` 으로 구독하면 `KRX:005930`)
 * - [quantity] 는 이 체결의 수량, [cumulativeVolume] 은 당일 누적 거래량
 * - 호가·등락은 프레임에 있으면 채우고 없으면 null
 */
data class TradeTick(
    val symbol: String,
    val price: BigDecimal,
    val quantity: BigDecimal,
    val timestamp: Instant,
    val bidPrice: BigDecimal? = null,
    val askPrice: BigDecimal? = null,
    val cumulativeVolume: Long? = null,
    val change: BigDecimal? = null,
    val changeRate: BigDecimal? = null,
) {
    /** 스트림 틱을 REST 현재가와 같은 모양으로 — 엔진이 quotes 호출을 아낄 때 쓴다 */
    fun toQuote(): Quote = Quote(
        symbol = symbol,
        price = price,
        bidPrice = bidPrice,
        askPrice = askPrice,
        volume = cumulativeVolume ?: 0L,
        change = change,
        changeRate = changeRate,
        timestamp = timestamp,
    )
}

fun interface TradeListener {
    fun onTrade(tick: TradeTick)
}

/**
 * 실시간 시장 데이터 스트림.
 *
 * 규약:
 * - [connect] 후 연결이 끊기면 스스로 지수 백오프로 재연결하고, 재연결 시 기존 구독을 다시 보낸다
 * - [subscribeTrades] 는 연결 전에 불러도 된다 — 연결되는 순간 전송된다
 * - 리스너는 스트림 스레드에서 호출된다. 오래 걸리는 일은 리스너 안에서 하지 말 것 (엔진은 스케줄러로 넘긴다)
 * - 리스너가 던진 예외는 스트림이 삼키고 로그만 남긴다 — 한 리스너 오류가 연결을 끊지 않는다
 * - [close] 뒤에는 재연결하지 않는다
 */
interface MarketStream : AutoCloseable {

    /** 현재 소켓이 열려 있고 (브로커가 요구하면) 로그인까지 끝났는지 */
    val isConnected: Boolean

    fun connect()

    fun subscribeTrades(symbols: List<String>, listener: TradeListener)

    override fun close()
}

/**
 * 실시간 스트림을 제공하는 브로커 어댑터. [BrokerCapabilities.streams] 가 비어있지 않은 어댑터만 구현한다.
 * 엔진은 `brokerClient is StreamingBrokerClient` 와 capabilities 둘 다 확인한다.
 */
interface StreamingBrokerClient : BrokerClient {

    /** 새 스트림 인스턴스를 만든다. 연결은 호출자가 [MarketStream.connect] 로 시작한다 */
    fun openStream(): MarketStream
}
