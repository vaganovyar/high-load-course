package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.web.reactive.function.client.WebClientRequestException
import org.springframework.web.reactive.function.client.WebClientResponseException
import ru.quipy.config.*
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.ConnectException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.absoluteValue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    threadPoolsConfig: ThreadPoolsConfig,
    private val metricsService: MetricsService,
    retriesConfig: RetriesConfig,
    private val logConfig: LogConfig,
    private val backPressureConfig: BackPressureConfig,
    private val queueSimulationConfig: QueueSimulationConfig,
    private val hedgedRequestsConfig: HedgedRequestsConfig,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        private const val TASK_NAME = "paymentTask"
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val queueSimulationEnabled = queueSimulationConfig.enabled

    private val maxParallelRequestsCount =
        maxOf(parallelRequests, rateLimitPerSec * maxOf(1, requestAverageProcessingTime.toSeconds().toInt()))

    private val circuitBreaker = CircuitBreaker.of(
        "payment-$accountName",
        CircuitBreakerConfig.custom()
            .slidingWindowType(SlidingWindowType.TIME_BASED)
            .slidingWindowSize(2) // За какие N секунд считаем вызовы
            .failureRateThreshold(30.0f)
            .minimumNumberOfCalls(10) // Минимальное число вызовов в окне, прежде чем оценивать threshold
            .waitDurationInOpenState(Duration.ofSeconds(3)) // Сколько остаётся в состоянии OPEN, прежде чем перейти в HALF_OPEN и попробовать пропустить несколько запросов
            .permittedNumberOfCallsInHalfOpenState(3) // Сколько запросов разрешено пропустить в HALF_OPEN, чтобы проверить ожил ли сервис
            .build()
    )

    private val requestQueue: PaymentChannel = PaymentChannel(retriesConfig.payment.maxRetries)

    private val executorThreadNumber = threadPoolsConfig.paymentExternalServiceThreadPoolConfig.threadsNumber
    private val executorService = Executors.newFixedThreadPool(executorThreadNumber)
    private val coroutineScope = CoroutineScope(executorService.asCoroutineDispatcher() + SupervisorJob())

    private val backgroundExecutorThreadNumber = threadPoolsConfig.paymentExternalServiceThreadPoolConfig.backgroundTaskThreadsNumber
    private val backgroundExecutorService = Executors.newFixedThreadPool(backgroundExecutorThreadNumber) as ThreadPoolExecutor

    private val requestHandlerThreadNumber = threadPoolsConfig.paymentExternalServiceThreadPoolConfig.requestHandlerThreadsNumber
    private val requestHandlerExecutorService = Executors.newFixedThreadPool(requestHandlerThreadNumber) as ThreadPoolExecutor

    private val maxConcurrentRequests: Int = parallelRequests

    @Volatile
    private var lastRetryAfterTimestamp: Long? = null

    private val queueLock = ReentrantLock()

    @Volatile
    private var lastQueueLogTime: Long = 0

    private val timeToProcessCounter =
        TimeToProcessCounter(requestAverageProcessingTime, maxConcurrentRequests, rateLimitPerSec)

    private val parallelIdempotencyRequestsExecutorService =
        ParallelIdempotencyRequestsExecutorService(
            threadPoolsConfig = threadPoolsConfig,
            tries = hedgedRequestsConfig.tries,
            delayTime = hedgedRequestsConfig.delay,
            properties = properties,
            logConfig = logConfig,
        )

    init {
        metricsService.registerQueueSizeGauge(accountName, this) { requestQueue.size().toDouble() }

        repeat(maxParallelRequestsCount) {
            coroutineScope.launch {
                processRequestQueue()
            }
        }
    }

    override fun performPaymentAsync(
        paymentId: UUID,
        orderId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        // Light calculations and queue size checks
        val checkQueueAndDeadline = {
            var currentTime = now()

            lastRetryAfterTimestamp?.let { retryAfter ->
                if (currentTime < retryAfter) {
                    logger.warn("[$accountName] TooManyRequestsException for payment $paymentId, retry-after time $retryAfter ms (from previous rejection)")
                    throw TooManyRequestsException("Too many requests", retryAfter)
                }
            }

            val timeRemaining = deadline - currentTime

            val timeNeededToProcessQueueWithNewRequest = timeToProcessCounter.computeTimeToProcessQueue(requestQueue.size() + 1).toMillis()

            if (timeRemaining <= 0 || (timeNeededToProcessQueueWithNewRequest + 100 > timeRemaining && queueSimulationEnabled)) {
                lastRetryAfterTimestamp = currentTime + timeNeededToProcessQueueWithNewRequest

                logger.warn("[$accountName] TooManyRequestsException for paymemt $paymentId, retry-after time $lastRetryAfterTimestamp ms, queue size ${requestQueue.size()}, time needed $timeNeededToProcessQueueWithNewRequest")
                throw TooManyRequestsException("Too many requests", lastRetryAfterTimestamp)
            }
        }

        // Execute checks under lock if precise timing is enabled
        if (backPressureConfig.usePreciseQueueProcessTime) {
            queueLock.withLock {
                checkQueueAndDeadline()
            }
        } else {
            checkQueueAndDeadline()
        }

        // Heavy operations in separate thread pool
        requestHandlerExecutorService.submit {
            paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }

            val transactionId = UUID.randomUUID()
            val request = PaymentRequest(paymentId, orderId, amount, paymentStartedAt, deadline, transactionId)
            runBlocking { requestQueue.offer(request) }
            log(
                paymentId,
                "[$accountName] Submitted payment request into queue $paymentId, time spent ${now() - paymentStartedAt} ms",
            )

            // Log queue size every second
            val currentTime = now()
            if (currentTime - lastQueueLogTime >= 1000) {
                lastQueueLogTime = currentTime
                logger.info("[$accountName] Queue size: ${requestQueue.size()}, background queue size: ${backgroundExecutorService.queue.size}, request handler queue size: ${requestHandlerExecutorService.queue.size}")
            }
        }
    }

    private suspend fun processRequestQueue() {
        while (true) {
            val request = requestQueue.take()

            if (now() + requestAverageProcessingTime.toMillis() + 100 > request.deadline && queueSimulationEnabled) {
                continue
            }

            processPaymentRequest(request)
            // TODO: nice idea but token returns into new window and rate limit is breached for good requests
            // val currentTime = now()
            // if (currentTime + requestAverageProcessingTime.toMillis() > request.deadline) {
            //     rateLimiter.returnToken()
            //     logger.warn("[$accountName] Request ${request.paymentId} missed deadline, returning token to rate limiter")
            // } else {
            //     processPaymentRequest(request)
            // }

            backgroundExecutorService.submit {
                metricsService.incrementCompletedTask(TASK_NAME)
            }
        }
    }

    private suspend fun processPaymentRequest(request: PaymentRequest) {
        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        withContext(Dispatchers.IO) {
            paymentESService.update(request.paymentId) {
                it.logSubmission(
                    success = true,
                    request.transactionId,
                    now(),
                    (now() - request.paymentStartedAt).milliseconds.toJavaDuration()
                )
            }
        }

        val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=${request.transactionId}&paymentId=${request.paymentId}&amount=${request.amount}"

        if (!circuitBreaker.tryAcquirePermission()) {
            requestQueue.offer(request)
            return
        }

        val requestStartTime = now()
        try {
            val responseData: Pair<String, Int>? =
                try {
                    log(
                        request.paymentId,
                        "[$accountName] Submit: ${request.paymentId} , txId: ${request.transactionId}, time spent ${now() - request.paymentStartedAt} ms"
                    )
                    val response = parallelIdempotencyRequestsExecutorService.executePaymentCall(url, request.paymentId)

                    Pair(response.body ?: "", response.statusCode.value())
                } catch (e: Exception) {
                    circuitBreaker.onError(now() - requestStartTime, TimeUnit.MILLISECONDS, e)
                    when (e) {
                        is TimeoutException, is TimeoutCancellationException -> {
                            log(
                                request.paymentId,
                                "[$accountName] Payment timeout for txId: ${request.transactionId}, payment: ${request.paymentId}, time spent ${requestAverageProcessingTime.toMillis()} ms",
                            )
                            val newRequest = request.copy(retries = request.retries + 1)
                            requestQueue.offer(newRequest)
                            null
                        }

                        is WebClientRequestException, is ConnectException, is IOException -> {
                            log(
                                request.paymentId,
                                "[$accountName] Connection error for txId: ${request.transactionId}, payment: ${request.paymentId}, error: ${e.message}",
                            )
                            delay(50)
                            val newRequest = request.copy(retries = request.retries + 1)
                            requestQueue.offer(newRequest)
                            null
                        }

                        is WebClientResponseException -> {
                            Pair(e.responseBodyAsString, e.statusCode.value())
                        }

                        else -> throw e
                    }
                }
            responseData?.let { (bodyString, statusCode) ->
                if (statusCode in 200..299) {
                    circuitBreaker.onSuccess(now() - requestStartTime, TimeUnit.MILLISECONDS)
                } else {
                    circuitBreaker.onError(
                        now() - requestStartTime,
                        TimeUnit.MILLISECONDS,
                        IllegalStateException("Non-2xx response: $statusCode")
                    )
                }
                val responseTime = now() - requestStartTime
                backgroundExecutorService.submit {
                    metricsService.recordExternalSystemResponseTime(accountName, responseTime)
                }
                val body =
                    try {
                        mapper.readValue(bodyString, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        log(
                            request.paymentId,
                            "[$accountName] [ERROR] Payment processed for txId: ${request.transactionId}, payment: ${request.paymentId}, result code: $statusCode, reason: ${e.message}"
                        )
                        ExternalSysResponse(
                            request.transactionId.toString(),
                            request.paymentId.toString(),
                            false,
                            e.message
                        )
                    }
                backgroundExecutorService.submit {
                    metricsService.registerExternalSystemRetries(accountName, request.retries)
                }

                log(
                    request.paymentId,
                    "[$accountName] Payment processed for txId: ${request.transactionId}, payment: ${request.paymentId}, succeeded: ${body.result}, result code: $statusCode, message: ${body.message}, time spent ${now() - request.paymentStartedAt} ms",
                )

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                backgroundExecutorService.submit {
                    paymentESService.update(request.paymentId) {
                        it.logProcessing(body.result, now(), request.transactionId, reason = body.message)
                    }
                }
            }

        } catch (e: Exception) {
            circuitBreaker.onError(now() - requestStartTime, TimeUnit.MILLISECONDS, e)
            backgroundExecutorService.submit {
                val responseTime = now() - requestStartTime
                metricsService.recordExternalSystemResponseTime(accountName, responseTime)
            }
            when (e) {
                else -> {
                    err(
                        request.paymentId,
                        "[$accountName] Payment failed for txId: ${request.transactionId}, payment: ${request.paymentId}, time spent ${now() - request.paymentStartedAt} ms",
                        e
                    )
                    backgroundExecutorService.submit {
                        paymentESService.update(request.paymentId) {
                            it.logProcessing(false, now(), request.transactionId, reason = e.message)
                        }
                    }
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun log(paymentId: UUID, msg: String) {
        if (logConfig.enabled && paymentId.hashCode().absoluteValue % logConfig.sample == 0) {
            logger.info(msg)
        }
    }

    private fun err(paymentId: UUID, msg: String, e: Throwable) {
        if (logConfig.enabled && paymentId.hashCode().absoluteValue % logConfig.sample == 0) {
            logger.error(msg, e)
        }
    }
}

public fun now() = System.currentTimeMillis()