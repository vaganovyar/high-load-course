package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okio.withLock
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.config.LogConfig
import ru.quipy.config.RetriesConfig
import ru.quipy.config.ThreadPoolsConfig
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.locks.ReentrantLock
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.math.absoluteValue
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
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
    private val logConfig: LogConfig
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()

        private const val TASK_NAME = "paymentTask"
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val maxParallelRequestsCount =
        maxOf(parallelRequests, rateLimitPerSec * maxOf(1, requestAverageProcessingTime.toSeconds().toInt()))


    private val client = OkHttpClient.Builder()
        .callTimeout(requestAverageProcessingTime.multipliedBy(2))
        .readTimeout(requestAverageProcessingTime.multipliedBy(2))
        .connectTimeout(requestAverageProcessingTime.multipliedBy(2))
        .writeTimeout(requestAverageProcessingTime.multipliedBy(2))
        .dispatcher(Dispatcher().apply {
            maxRequests = maxParallelRequestsCount
            maxRequestsPerHost = maxParallelRequestsCount
        })
        .build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), 1.seconds.toJavaDuration())
    private val requestQueue: PaymentChannel = PaymentChannel(retriesConfig.payment.maxRetries)

    private val executorThreadNumber = threadPoolsConfig.paymentExternalServiceThreadPoolConfig.threadsNumber
    private val executorService = Executors.newFixedThreadPool(executorThreadNumber)
    private val coroutineScope = CoroutineScope(executorService.asCoroutineDispatcher() + SupervisorJob())

    private val backgroundExecutorThreadNumber = threadPoolsConfig.paymentExternalServiceThreadPoolConfig.backgroundTaskThreadsNumber
    private val backgroundExecutorService = Executors.newFixedThreadPool(backgroundExecutorThreadNumber) as ThreadPoolExecutor

    private val maxConcurrentRequests: Int = parallelRequests
    private val requestSemaphore = Semaphore(permits = maxConcurrentRequests)

    @Volatile
    private var lastRetryAfterTimestamp: Long? = null

    private val queueLock = ReentrantLock()
    
    @Volatile
    private var lastQueueLogTime: Long = 0

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
        val transactionId = UUID.randomUUID()

        paymentESService.create {
            it.create(
                paymentId,
                orderId,
                amount
            )
        }

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
            logger.info("[$accountName] Queue size: ${requestQueue.size()}, background queue size: ${backgroundExecutorService.queue.size}")
        }
    }

    private suspend fun processRequestQueue() {
        while (true) {
            val request = requestQueue.take()

            if (now() + requestAverageProcessingTime.toMillis() + 100 > request.deadline) {
                continue
            }

            requestSemaphore.withPermit {
                rateLimiter.tickSuspend()
                processPaymentRequest(request)
            }

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

        val httpRequest = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=${request.transactionId}&paymentId=${request.paymentId}&amount=${request.amount}")
            post(emptyBody)
        }.build()

        val requestStartTime = now()
        try {
            val response: Response? =
                try {
                    val call = client.newCall(httpRequest)
                    log(
                        request.paymentId,
                        "[$accountName] Submit: ${request.paymentId} , txId: ${request.transactionId}, time spent ${now() - request.paymentStartedAt} ms"
                    )
                    call.await()
                } catch (e: Exception) {
                    when (e) {
                        is InterruptedIOException, is TimeoutCancellationException -> {
                            log(
                                request.paymentId,
                                "[$accountName] Payment timeout for txId: ${request.transactionId}, payment: ${request.paymentId}, time spent ${requestAverageProcessingTime.toMillis()} ms",
                            )
                            val newRequest = request.copy(retries = request.retries + 1)
                            requestQueue.offer(newRequest)
                            null
                        }

                        else -> throw e
                    }
                }
            val responseTime = now() - requestStartTime
            backgroundExecutorService.submit {
                metricsService.recordExternalSystemResponseTime(accountName, responseTime)
            }
            response?.let { resp ->
                val body =
                    try {
                        val bodyString = withContext(Dispatchers.IO) {
                            resp.body?.string()
                        }
                        mapper.readValue(bodyString, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        log(
                            request.paymentId,
                            "[$accountName] [ERROR] Payment processed for txId: ${request.transactionId}, payment: ${request.paymentId}, result code: ${resp.code}, reason: ${resp.body?.string()}"
                        )
                        ExternalSysResponse(
                            request.transactionId.toString(),
                            request.paymentId.toString(),
                            false,
                            e.message
                        )
                    } finally {
                        resp.close()
                    }
                backgroundExecutorService.submit {
                    metricsService.registerExternalSystemRetries(accountName, request.retries)
                }

                log(
                    request.paymentId,
                    "[$accountName] Payment processed for txId: ${request.transactionId}, payment: ${request.paymentId}, succeeded: ${body.result}, result code: ${resp.code}, message: ${body.message}, time spent ${now() - request.paymentStartedAt} ms",
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

    private fun log(paymentId: UUID , msg: String) {
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

private suspend fun okhttp3.Call.await(): Response {
    return suspendCancellableCoroutine { continuation ->
        enqueue(object : okhttp3.Callback {
            override fun onResponse(call: okhttp3.Call, response: Response) {
                continuation.resume(response)
            }

            override fun onFailure(call: okhttp3.Call, e: java.io.IOException) {
                continuation.resumeWithException(e)
            }
        })

        continuation.invokeOnCancellation {
            try {
                cancel()
            } catch (ex: Exception) {
            }
        }
    }
}

public fun now() = System.currentTimeMillis()