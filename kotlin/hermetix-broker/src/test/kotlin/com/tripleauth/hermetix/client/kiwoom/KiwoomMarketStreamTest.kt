package com.tripleauth.hermetix.client.kiwoom

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.TradeTick
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class KiwoomMarketStreamTest {

    private val objectMapper = ObjectMapper().registerModule(kotlinModule())
    private lateinit var server: MockWebServer
    private var stream: KiwoomMarketStream? = null

    private class ServerConnection(val socket: WebSocket) {
        val received = LinkedBlockingQueue<String>()
    }

    private val connections = LinkedBlockingQueue<ServerConnection>()
    private val allConnections = java.util.concurrent.CopyOnWriteArrayList<ServerConnection>()

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse =
                MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                    lateinit var conn: ServerConnection
                    override fun onOpen(webSocket: WebSocket, response: Response) {
                        conn = ServerConnection(webSocket).also { allConnections += it; connections.put(it) }
                    }
                    override fun onMessage(webSocket: WebSocket, text: String) {
                        conn.received.put(text)
                    }
                    // OkHttp 서버 소켓은 close 프레임을 받아도 앱이 close() 로 응답해야 핸드셰이크가 끝난다 — 안 하면 shutdown 이 5초 대기
                    override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                        webSocket.close(code, reason)
                    }
                })
        }
        server.start()
    }

    @AfterEach
    fun tearDown() {
        stream?.close()
        allConnections.forEach { runCatching { it.socket.close(1000, null) } }
        server.shutdown()
    }

    private fun newStream(): KiwoomMarketStream =
        KiwoomMarketStream(
            KiwoomApiProperties(wsUrl = "ws://${server.hostName}:${server.port}/api/dostk/websocket"),
            objectMapper,
            token = { "ACCESS-TOKEN" },
        ).also { stream = it }

    private fun <T> LinkedBlockingQueue<T>.take(seconds: Long = 5): T =
        poll(seconds, TimeUnit.SECONDS) ?: error("${seconds}s 안에 도착하지 않음")

    @Test
    fun `접속 → LOGIN → REG 순서로 진행하고 REAL 프레임을 틱으로 전달한다`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val s = newStream()
        s.subscribeTrades(listOf("KRX:005930")) { ticks.put(it) }
        s.connect()

        val conn = connections.take()
        val login = objectMapper.readTree(conn.received.take())
        assertThat(login.path("trnm").asText()).isEqualTo("LOGIN")
        assertThat(login.path("token").asText()).isEqualTo("ACCESS-TOKEN")
        assertThat(s.isConnected).isFalse() // 로그인 응답 전

        conn.socket.send("""{"trnm":"LOGIN","return_code":0,"return_msg":""}""")
        val reg = objectMapper.readTree(conn.received.take())
        assertThat(reg.path("trnm").asText()).isEqualTo("REG")
        assertThat(reg.path("data")[0].path("item")[0].asText()).isEqualTo("005930")
        assertThat(reg.path("data")[0].path("type")[0].asText()).isEqualTo("0B")

        conn.socket.send("""{"trnm":"REG","return_code":0,"return_msg":""}""")
        conn.socket.send(REAL_FRAME)

        val tick = ticks.take()
        assertThat(tick.symbol).isEqualTo("KRX:005930")
        assertThat(tick.price).isEqualByComparingTo("71500")
        assertThat(tick.quantity).isEqualByComparingTo("15")
        assertThat(s.isConnected).isTrue()
    }

    @Test
    fun `PING 은 받은 그대로 돌려보낸다`() {
        val s = newStream()
        s.connect()
        val conn = connections.take()
        conn.received.take() // LOGIN
        val ping = """{"trnm":"PING"}"""
        conn.socket.send(ping)
        assertThat(conn.received.take()).isEqualTo(ping)
    }

    @Test
    fun `서버가 끊으면 재접속해 다시 로그인하고 등록한다`() {
        val s = newStream()
        s.subscribeTrades(listOf("005930")) { }
        s.connect()
        val first = connections.take()
        first.received.take() // LOGIN
        first.socket.close(1000, "bye")

        val second = connections.take(10)
        assertThat(objectMapper.readTree(second.received.take()).path("trnm").asText()).isEqualTo("LOGIN")
        second.socket.send("""{"trnm":"LOGIN","return_code":0}""")
        assertThat(objectMapper.readTree(second.received.take()).path("trnm").asText()).isEqualTo("REG")
    }

    @Test
    fun `REAL 파싱 - 부호 제거, A 프리픽스 제거, 0B 외 타입 무시`() {
        val today = LocalDate.of(2026, 9, 14)
        val tick = KiwoomMarketStream.parseReal(objectMapper.readTree(REAL_FRAME), today).single()
        assertThat(tick.symbol).isEqualTo("005930")
        assertThat(tick.price).isEqualByComparingTo("71500")
        assertThat(tick.askPrice).isEqualByComparingTo("71500")
        assertThat(tick.bidPrice).isEqualByComparingTo("71400")
        assertThat(tick.quantity).isEqualByComparingTo("15")
        assertThat(tick.cumulativeVolume).isEqualTo(1234567L)
        assertThat(tick.change).isEqualByComparingTo("-300")
        assertThat(tick.changeRate).isEqualByComparingTo("-0.0042")
        assertThat(tick.timestamp.atZone(ZoneId.of("Asia/Seoul")).toLocalTime().toString()).isEqualTo("09:30:12")

        val orderbook = """{"trnm":"REAL","data":[{"type":"0D","name":"주식호가잔량","item":"005930","values":{"21":"093012"}}]}"""
        assertThat(KiwoomMarketStream.parseReal(objectMapper.readTree(orderbook), today)).isEmpty()
    }

    companion object {
        const val REAL_FRAME = """{"trnm":"REAL","data":[{"type":"0B","name":"주식체결","item":"A005930","values":{"20":"093012","10":"-71500","11":"-300","12":"-0.42","27":"+71500","28":"+71400","15":"-15","13":"1234567","14":"88000000000","16":"71800","17":"71900","18":"71300"}}]}"""
    }
}
