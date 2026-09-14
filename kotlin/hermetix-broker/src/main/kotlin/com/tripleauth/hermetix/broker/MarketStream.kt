package com.tripleauth.hermetix.broker

import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.Quote
import java.math.BigDecimal
import java.time.Instant

/**
 * 브로커가 제공하는 실시간 스트림 채널. [BrokerCapabilities.streams] 로 선언한다.
 */
enum class StreamChannel {
    /** 체결가 — 체결이 일어날 때마다 [TradeTick] */
    TRADES,
    /** 호가 — 호가창이 바뀔 때마다 [OrderBookTick] (10단계) */
    ORDER_BOOK,
    /** 내 주문의 접수·체결·취소·거부 통보 — [OrderEvent] */
    ORDER_EVENTS,
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

/** 호가 한 단계 */
data class OrderBookLevel(
    val price: BigDecimal,
    val quantity: BigDecimal,
)

/**
 * 호가창 스냅샷. [asks]/[bids] 는 최우선(1호가)부터 순서대로, 브로커가 주는 만큼(보통 10단계).
 * 심볼은 구독 요청 표기 그대로.
 */
data class OrderBookTick(
    val symbol: String,
    val timestamp: Instant,
    val asks: List<OrderBookLevel>,
    val bids: List<OrderBookLevel>,
    val totalAskQuantity: BigDecimal? = null,
    val totalBidQuantity: BigDecimal? = null,
) {
    val bestAsk: OrderBookLevel? get() = asks.firstOrNull()
    val bestBid: OrderBookLevel? get() = bids.firstOrNull()
}

enum class OrderEventType {
    /** 주문 접수 */
    ACCEPTED,
    /** 체결 (부분 체결 포함 — [OrderEvent.quantity] 가 이번 체결량) */
    FILLED,
    /** 취소 확인 */
    CANCELED,
    /** 정정 확인 */
    MODIFIED,
    /** 거부 */
    REJECTED,
}

/**
 * 내 주문 통보 1건.
 *
 * - [orderId] 는 브로커 주문번호. 브로커에 따라 REST 응답과 자릿수(0 패딩)가 다를 수 있어 비교는 [orderIdMatches] 로 한다
 * - [quantity]/[price] 는 이벤트 종류에 따라 체결량·체결가(FILLED) 또는 주문량·주문가(그 외)
 * - [remainingQuantity] 는 브로커가 주는 경우만 (키움 902). KIS 통보에는 없다
 */
data class OrderEvent(
    val orderId: String,
    val type: OrderEventType,
    val timestamp: Instant,
    val symbol: String? = null,
    val side: OrderSide? = null,
    val quantity: BigDecimal? = null,
    val price: BigDecimal? = null,
    val remainingQuantity: BigDecimal? = null,
    val originalOrderId: String? = null,
    val reason: String? = null,
) {
    /** 앞자리 0 패딩 차이를 무시한 주문번호 비교 (KIS 통보 10자리 vs REST ODNO 7자리 등) */
    fun orderIdMatches(other: String): Boolean = normalizeOrderId(orderId) == normalizeOrderId(other)

    companion object {
        fun normalizeOrderId(id: String): String = id.trim().trimStart('0').ifEmpty { "0" }
    }
}

fun interface TradeListener {
    fun onTrade(tick: TradeTick)
}

fun interface OrderBookListener {
    fun onOrderBook(tick: OrderBookTick)
}

fun interface OrderEventListener {
    fun onOrderEvent(event: OrderEvent)
}

/**
 * 실시간 시장 데이터 스트림.
 *
 * 규약:
 * - [connect] 후 연결이 끊기면 스스로 지수 백오프로 재연결하고, 재연결 시 기존 구독을 다시 보낸다
 * - subscribe* 는 연결 전에 불러도 된다 — 연결되는 순간 전송된다
 * - 리스너는 스트림 스레드에서 호출된다. 오래 걸리는 일은 리스너 안에서 하지 말 것 (엔진은 스케줄러로 넘긴다)
 * - 리스너가 던진 예외는 스트림이 삼키고 로그만 남긴다 — 한 리스너 오류가 연결을 끊지 않는다
 * - [close] 뒤에는 재연결하지 않는다
 * - 선언하지 않은 채널([BrokerCapabilities.streams])의 subscribe 는 [UnsupportedOperationException]
 */
interface MarketStream : AutoCloseable {

    /** 현재 소켓이 열려 있고 (브로커가 요구하면) 로그인까지 끝났는지 */
    val isConnected: Boolean

    fun connect()

    fun subscribeTrades(symbols: List<String>, listener: TradeListener)

    fun subscribeOrderBook(symbols: List<String>, listener: OrderBookListener) {
        throw UnsupportedOperationException("이 브로커는 호가 스트림을 제공하지 않습니다")
    }

    /** 계좌 전체의 주문 통보 — 심볼 지정 없음 */
    fun subscribeOrderEvents(listener: OrderEventListener) {
        throw UnsupportedOperationException("이 브로커는 주문 통보 스트림을 제공하지 않습니다")
    }

    override fun close()
}

/**
 * 실시간 스트림을 제공하는 브로커 어댑터. [BrokerCapabilities.streams] 가 비어있지 않은 어댑터만 구현한다.
 * 엔진은 `brokerClient is StreamingBrokerClient` 와 capabilities 둘 다 확인한다.
 */
interface StreamingBrokerClient : BrokerClient {

    /** 새 스트림 인스턴스를 만든다. 연결은 호출자가 [MarketStream.connect] 로 시작한다 */
    fun openStream(): MarketStream

    /**
     * 엔진이 받은 주문 통보를 어댑터에 전달한다. 서버 주문 조회가 없어 메모리로 추적하는 어댑터(KIS 모의)는
     * 여기서 체결·취소를 반영해 [BrokerClient.getOrder] 가 즉시 맞는 상태를 돌려주게 한다. 기본은 아무것도 안 한다
     */
    fun applyOrderEvent(event: OrderEvent) {}
}
