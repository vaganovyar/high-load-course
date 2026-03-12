package ru.quipy.payments.logic

import kotlinx.coroutines.*
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import reactor.netty.http.HttpProtocol
import reactor.netty.http.client.HttpClient
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.config.LogConfig
import ru.quipy.config.ThreadPoolsConfig
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.absoluteValue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class ParallelIdempotencyRequestsExecutorService(
    private val threadPoolsConfig: ThreadPoolsConfig,
    private val tries: Int,
    private val delayTime: Long,
    private val properties: PaymentAccountProperties,
    private val logConfig: LogConfig,
) {
    private val logger = LoggerFactory.getLogger(ParallelIdempotencyRequestsExecutorService::class.java)

    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val maxParallelRequestsCount =
        maxOf(parallelRequests, rateLimitPerSec * maxOf(1, requestAverageProcessingTime.toSeconds().toInt()))

    private val httpClient = HttpClient.create()
        .protocol(HttpProtocol.H2C)
        .responseTimeout(Duration.ofMillis(maxOf(requestAverageProcessingTime.multipliedBy(2).toMillis(), 500)))
    private val webClient = WebClient.builder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), 1.seconds.toJavaDuration())
    private val requestSemaphore = Semaphore(permits = parallelRequests)

    suspend fun executePaymentCall(url: String, paymentId: UUID): ResponseEntity<String> =
        coroutineScope {
            val idempotencyKey = UUID.randomUUID().toString()
            val result = CompletableDeferred<ResponseEntity<String>>()
            val lastCall = AtomicLong(now() - 2 * delayTime)

            val jobs = (0 until tries).map { attemptIndex ->
                launch {
                    try {
                        requestSemaphore.withPermit {
                            rateLimiter.tickSuspend()
                            while (true) {
                                val currentLastCall = lastCall.get()
                                val delta = now() - currentLastCall
                                if (delta > delayTime) {
                                    if (lastCall.compareAndSet(currentLastCall, now())) {
                                        break
                                    }
                                } else {
                                    delay(delta)
                                }
                            }
                            log(paymentId, "Starting call $attemptIndex")
                            val response = webClient.post()
                                .uri(url)
                                .header("x-idempotency-key", idempotencyKey)
                                .retrieve()
                                .toEntity(String::class.java)
                                .onErrorResume { e ->
                                    if (e is IllegalStateException &&
                                        e.message?.contains("response body has been released already") == true
                                    ) {
                                        Mono.empty()
                                    } else {
                                        Mono.error(e)
                                    }
                                }
                                .awaitSingleOrNull()

                            if (response == null) {
                                throw CancellationException("Response body released due to cancellation")
                            }

                            if (response.statusCode.is2xxSuccessful) {
                                if (result.complete(response)) {
                                    log(paymentId, "Successful attempt: $attemptIndex")
                                }
                            }
                        }
                    } catch (_: CancellationException) {
                    } catch (e: Exception) {
                        log(paymentId, "Attempt $attemptIndex failed: ${e.message}")
                    }
                }
            }

            try {
                withTimeout(requestAverageProcessingTime.toMillis() +
                            delayTime * tries) {
                    result.await()
                }
            } catch (e: TimeoutCancellationException) {
                throw TimeoutException("Payment request timed out")
            } finally {
                jobs.forEach { it.cancel() }
            }
        }

    private fun log(paymentId: UUID, msg: String) {
        if (logConfig.enabled && paymentId.hashCode().absoluteValue % logConfig.sample == 0) {
            logger.info("[$paymentId] $msg")
        }
    }
}