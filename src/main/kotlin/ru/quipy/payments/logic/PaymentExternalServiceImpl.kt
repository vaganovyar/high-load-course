package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.config.ThreadPoolsConfig
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import kotlin.math.ceil
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

data class PaymentRequest(
    val paymentId: UUID,
    val orderId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
    val transactionId: UUID
)

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val threadPoolsConfig: ThreadPoolsConfig,
    private val metricsService: MetricsService
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()

        private const val TASK_NAME = "paymentTask"

        val retryForbiddenCodes = hashSetOf(
            // 4xx - клиентские ошибки (бессмысленно ретраить)
            400, // Bad Request - ошибка в запросе
            401, // Unauthorized - нужна аутентификация
            403, // Forbidden - доступ запрещен
            404, // Not Found - ресурс не найден
            405, // Method Not Allowed - метод не поддерживается
            409, // Conflict - конфликт состояний
            410, // Gone - ресурс удален
            422, // Unprocessable Entity - семантическая ошибка

            // 429 - Too Many Requests (ретраить нельзя - усугубит)
            429,

            // 5xx - некоторые серверные ошибки (бессмысленно ретраить)
            501, // Not Implemented - функционал не реализован
            502, // Bad Gateway - проблемы прокси
            503, // Service Unavailable - сервис недоступен
            504  // Gateway Timeout - таймаут шлюза
        )
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), 1.seconds.toJavaDuration())
    private val requestQueue: BlockingQueue<PaymentRequest> = LinkedBlockingQueue()

    private val executorThreadNumber = threadPoolsConfig.paymentExternalServiceThreadPoolConfig.threadsNumber
    private val executorService = Executors.newFixedThreadPool(executorThreadNumber)
    private val coroutineScope = CoroutineScope(executorService.asCoroutineDispatcher() + SupervisorJob())

    private val maxConcurrentRequests: Int = parallelRequests
    private val requestSemaphore = Semaphore(permits = maxConcurrentRequests)

    @Volatile
    private var lastRetryAfterTimestamp: Long? = null

    private val queueLock = Any()
    private val retriedPayments = Collections.synchronizedSet(HashSet<UUID>())

    init {
        metricsService.registerQueueSizeGauge(accountName, this) { requestQueue.size.toDouble() }

        repeat(executorThreadNumber) {
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
        synchronized(queueLock) {
            val currentTime = now()

            lastRetryAfterTimestamp?.let { retryAfter ->
                if (currentTime < retryAfter) {
                    logger.warn("[$accountName] TooManyRequestsException for payment $paymentId, retry-after time $retryAfter ms (from previous rejection)")
                    throw TooManyRequestsException("Too many requests", retryAfter)
                }
            }

            val transactionId = UUID.randomUUID()
            val maxRpsFromConcurrency = maxConcurrentRequests.toDouble() / requestAverageProcessingTime.toMillis().toDouble() * 1000
            val effectiveRps = minOf(maxRpsFromConcurrency, rateLimitPerSec.toDouble())

            val timeRemaining = deadline - currentTime

            // requestAverageProcessingTime.toMillis() == to wait already sended requests
            // (requestQueue.size + 1 + effectiveRps) / effectiveRps == to process queue with current request added
            // ceil is needed because we need 2s to process 12 requests with 11rps (11 requests in first sec + 1 request in second sec)
            val timeNeededToProcessQueueWithNewRequest = ceil((requestQueue.size.toDouble() + 1) / effectiveRps).toLong() * 1000 + requestAverageProcessingTime.toMillis() // in milliseconds

            if (timeRemaining <= 0 || timeNeededToProcessQueueWithNewRequest + 100 > timeRemaining) {
                lastRetryAfterTimestamp = currentTime + timeNeededToProcessQueueWithNewRequest

                logger.warn("[$accountName] TooManyRequestsException for paymemt $paymentId, retry-after time $lastRetryAfterTimestamp ms, queue size ${requestQueue.size}, time needed $timeNeededToProcessQueueWithNewRequest")
                throw TooManyRequestsException("Too many requests", lastRetryAfterTimestamp)
            }

            if (!retriedPayments.contains(paymentId)) {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
            }

            val request = PaymentRequest(paymentId, orderId, amount, paymentStartedAt, deadline, transactionId)
            requestQueue.offer(request)
            logger.info("[$accountName] Submitted payment request into queue $paymentId, time spent ${currentTime - paymentStartedAt} ms, queue size ${requestQueue.size}, time needed $timeNeededToProcessQueueWithNewRequest")
        }
    }

    private suspend fun processRequestQueue() {
        while (true) {
            try {
                val request = requestQueue.take()

                if (now() + requestAverageProcessingTime.toMillis() + 100 > request.deadline) {
                    continue
                }

                requestSemaphore.withPermit {
                    withContext(Dispatchers.IO) {
                        rateLimiter.tickBlocking()
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
                }

                metricsService.incrementCompletedTask(TASK_NAME)
            } catch (e: Exception) {
                logger.error("[$accountName] Error processing request queue", e)
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

        logger.info("[$accountName] Submit: ${request.paymentId} , txId: ${request.transactionId}, time spent ${now() - request.paymentStartedAt} ms")

        val httpRequest = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=${request.transactionId}&paymentId=${request.paymentId}&amount=${request.amount}")
            post(emptyBody)
        }.build()

        val requestStartTime = now()
        try {
            val response = withContext(Dispatchers.IO) {
                client.newCall(httpRequest).execute()
            }
            val responseTime = now() - requestStartTime
            metricsService.recordExternalSystemResponseTime(accountName, responseTime)

            response.use { resp ->
                val body = try {
                    mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: ${request.transactionId}, payment: ${request.paymentId}, result code: ${resp.code}, reason: ${resp.body?.string()}")
                    ExternalSysResponse(
                        request.transactionId.toString(),
                        request.paymentId.toString(),
                        false,
                        e.message
                    )
                }

                logger.info("[$accountName] Payment processed for txId: ${request.transactionId}, payment: ${request.paymentId}, succeeded: ${body.result}, result code: ${resp.code}, message: ${body.message}, time spent ${now() - request.paymentStartedAt} ms")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                withContext(Dispatchers.IO) {
                    paymentESService.update(request.paymentId) {
                        it.logProcessing(body.result, now(), request.transactionId, reason = body.message)
                    }
                }
                
                // Если это был ретрай, удаляем payment из HashSet
                if (retriedPayments.contains(request.paymentId)) {
                    retriedPayments.remove(request.paymentId)
                } else if (!body.result && !retryForbiddenCodes.contains(resp.code)) {
                    // Если результат неуспешный и для этого payment еще не было ретрая, пробуем сделать ретрай
                    retriedPayments.add(request.paymentId)
                    try {
                        performPaymentAsync(request.paymentId, request.orderId, request.amount, request.paymentStartedAt, request.deadline)
                        logger.info("[$accountName] Retry submitted for payment ${request.paymentId}, txId: ${request.transactionId}")
                    } catch (e: Exception) {
                        retriedPayments.remove(request.paymentId)
                        logger.warn("[$accountName] Retry failed for payment ${request.paymentId}, txId: ${request.transactionId}", e)
                    }
                }
            }
        } catch (e: Exception) {
            val responseTime = now() - requestStartTime
            metricsService.recordExternalSystemResponseTime(accountName, responseTime)
            
            when (e) {
                is SocketTimeoutException -> {
                    logger.error(
                        "[$accountName] Payment timeout for txId: ${request.transactionId}, payment: ${request.paymentId}, time spent ${now() - request.paymentStartedAt} ms",
                        e
                    )
                    withContext(Dispatchers.IO) {
                        paymentESService.update(request.paymentId) {
                            it.logProcessing(false, now(), request.transactionId, reason = "Request timeout.")
                        }
                    }
                }

                else -> {
                    logger.error(
                        "[$accountName] Payment failed for txId: ${request.transactionId}, payment: ${request.paymentId}, time spent ${now() - request.paymentStartedAt} ms",
                        e
                    )
                    withContext(Dispatchers.IO) {
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

}

public fun now() = System.currentTimeMillis()