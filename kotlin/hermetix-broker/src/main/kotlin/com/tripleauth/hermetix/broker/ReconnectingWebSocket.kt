package com.tripleauth.hermetix.broker

import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min

/**
 * 브로커 웹소켓 어댑터의 공용 부품 — JDK 내장 [java.net.http.WebSocket] 위에 재연결·유휴 감시·직렬 전송을 얹는다.
 * 외부 의존성 없음.
 *
 * 하위 클래스가 정하는 것: 접속 URI([uri]), 연결 직후 보낼 것([onOpen] — 로그인·구독), 텍스트 프레임 처리([onMessage]).
 *
 * 동작:
 * - [connect] 는 즉시 반환하고 전용 스레드에서 접속한다. 실패하면 1s → 2s → 4s … [maxBackoffMillis] 로 재시도
 * - 소켓이 닫히거나 오류가 나면 같은 백오프로 재접속한다. [close] 뒤에는 재접속하지 않는다
 * - [idleTimeoutMillis] 동안 프레임이 하나도 없으면 죽은 연결로 보고 끊고 재접속한다
 *   (브로커들이 주기적으로 PING 류 프레임을 보내므로 정상 연결에서는 발생하지 않는다)
 * - 부분 프레임은 합쳐서 완성된 메시지 단위로 [onMessage] 에 넘긴다
 * - [send] 는 직렬화된다 (JDK WebSocket 은 동시 sendText 를 허용하지 않는다)
 */
abstract class ReconnectingWebSocket(
    private val name: String,
    private val maxBackoffMillis: Long = 30_000,
    private val idleTimeoutMillis: Long = 90_000,
    connectTimeout: Duration = Duration.ofSeconds(10),
) : AutoCloseable {

    private val logger = KotlinLogging.logger { }

    private val httpClient: HttpClient = HttpClient.newBuilder().connectTimeout(connectTimeout).build()

    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "$name-ws").apply { isDaemon = true }
    }

    private val sendLock = Any()
    private val attempt = AtomicInteger(0)
    private val partial = StringBuilder()

    @Volatile
    private var socket: WebSocket? = null

    @Volatile
    private var closed = false

    @Volatile
    private var lastFrameAt: Long = System.currentTimeMillis()

    /** 소켓이 열려 있는지 — 로그인 필요 브로커는 하위 클래스가 별도 상태를 둔다 */
    @Volatile
    var isSocketOpen: Boolean = false
        private set

    /** 매 (재)접속마다 호출된다 — 토큰·승인키 갱신은 여기서 */
    protected abstract fun uri(): URI

    protected open fun headers(): Map<String, String> = emptyMap()

    /** 소켓이 열린 직후 (로그인/구독 전송). 스트림 스레드에서 호출된다 */
    protected abstract fun onOpen()

    /** 완성된 텍스트 프레임 1건. 스트림 스레드에서 호출된다 — 예외는 로그만 남긴다 */
    protected abstract fun onMessage(text: String)

    /** 연결이 끊긴 직후 (재접속 예약 전). 하위 클래스가 로그인 상태 등을 초기화한다 */
    protected open fun onDisconnected() {}

    fun connect() {
        check(!closed) { "$name stream: 닫힌 스트림은 다시 열 수 없다" }
        executor.execute { doConnect() }
        executor.scheduleAtFixedRate(::checkIdle, idleTimeoutMillis, idleTimeoutMillis / 3, TimeUnit.MILLISECONDS)
    }

    /** 텍스트 프레임 전송. 연결이 없으면 false */
    fun send(text: String): Boolean {
        val ws = socket ?: return false
        return try {
            synchronized(sendLock) { ws.sendText(text, true).join() }
            true
        } catch (e: Exception) {
            logger.warn { "$name stream: 전송 실패 - ${e.message}" }
            false
        }
    }

    override fun close() {
        closed = true
        val ws = socket
        socket = null
        isSocketOpen = false
        runCatching { ws?.sendClose(WebSocket.NORMAL_CLOSURE, "bye")?.get(2, TimeUnit.SECONDS) }
        runCatching { ws?.abort() }
        executor.shutdownNow()
    }

    // ------------------------------------------------------------------ internals

    private fun doConnect() {
        if (closed) return
        try {
            val builder = httpClient.newWebSocketBuilder()
            headers().forEach { (k, v) -> builder.header(k, v) }
            val target = uri()
            logger.info { "$name stream: connecting $target (attempt ${attempt.get() + 1})" }
            val ws = builder.buildAsync(target, Handler()).join()
            socket = ws
            isSocketOpen = true
            lastFrameAt = System.currentTimeMillis()
            attempt.set(0)
            logger.info { "$name stream: connected" }
            runCatching { onOpen() }.onFailure { logger.error(it) { "$name stream: onOpen 실패" } }
        } catch (e: Exception) {
            logger.warn { "$name stream: 접속 실패 - ${e.cause?.message ?: e.message}" }
            scheduleReconnect()
        }
    }

    private fun handleDisconnect(reason: String) {
        val wasOpen = isSocketOpen
        socket = null
        isSocketOpen = false
        if (closed) return
        if (wasOpen) logger.warn { "$name stream: disconnected - $reason" }
        runCatching { onDisconnected() }
        scheduleReconnect()
    }

    private fun scheduleReconnect() {
        if (closed || executor.isShutdown) return
        val n = attempt.getAndIncrement()
        val delay = min(1000L shl min(n, 10), maxBackoffMillis)
        logger.info { "$name stream: reconnect in ${delay}ms" }
        executor.schedule(::doConnect, delay, TimeUnit.MILLISECONDS)
    }

    private fun checkIdle() {
        val ws = socket ?: return
        if (closed) return
        if (System.currentTimeMillis() - lastFrameAt > idleTimeoutMillis) {
            logger.warn { "$name stream: ${idleTimeoutMillis}ms 동안 프레임 없음 - 재접속" }
            runCatching { ws.abort() }
            handleDisconnect("idle timeout")
        }
    }

    private fun dispatch(text: String) {
        try {
            onMessage(text)
        } catch (e: Exception) {
            logger.error(e) { "$name stream: 메시지 처리 실패 - ${text.take(200)}" }
        }
    }

    private inner class Handler : WebSocket.Listener {

        override fun onOpen(webSocket: WebSocket) {
            webSocket.request(1)
        }

        override fun onText(webSocket: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
            lastFrameAt = System.currentTimeMillis()
            partial.append(data)
            if (last) {
                val message = partial.toString()
                partial.setLength(0)
                dispatch(message)
            }
            webSocket.request(1)
            return null
        }

        override fun onBinary(webSocket: WebSocket, data: ByteBuffer, last: Boolean): CompletionStage<*>? {
            lastFrameAt = System.currentTimeMillis()
            webSocket.request(1)
            return null
        }

        override fun onPing(webSocket: WebSocket, message: ByteBuffer): CompletionStage<*>? {
            lastFrameAt = System.currentTimeMillis()
            webSocket.request(1)
            return null
        }

        override fun onPong(webSocket: WebSocket, message: ByteBuffer): CompletionStage<*>? {
            lastFrameAt = System.currentTimeMillis()
            webSocket.request(1)
            return null
        }

        override fun onClose(webSocket: WebSocket, statusCode: Int, reason: String): CompletionStage<*>? {
            executor.execute { handleDisconnect("close $statusCode ${reason.ifBlank { "" }}".trim()) }
            return null
        }

        override fun onError(webSocket: WebSocket, error: Throwable) {
            executor.execute { handleDisconnect("error ${error.message}") }
        }
    }
}
