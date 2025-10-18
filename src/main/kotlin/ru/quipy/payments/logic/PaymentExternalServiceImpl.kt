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
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

data class PaymentRequest(
    val paymentId: UUID,
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
        val transactionId = UUID.randomUUID()

        val maxRpsFromConcurrency = maxConcurrentRequests / requestAverageProcessingTime.seconds
        val effectiveRps = minOf(maxRpsFromConcurrency, rateLimitPerSec.toLong())

        val timeRemaining = deadline - now()
        val timeNeededToProcessQueue = requestQueue.size / effectiveRps * 1000 * 1.1 // in milliseconds

        if (timeRemaining <= 0 || timeNeededToProcessQueue > timeRemaining) {
            val retryAfterTimestamp = now() + (requestQueue.size / effectiveRps * 1000 * 1.1).toLong()

            logger.warn("[$accountName] TooManyRequestsException for paymemt $paymentId, retry-after time $retryAfterTimestamp ms")
            throw TooManyRequestsException("Too many requests", retryAfterTimestamp)
        }

        val createdEvent = paymentESService.create {
            it.create(
                paymentId,
                orderId,
                amount
            )
        }

        val request = PaymentRequest(paymentId, amount, paymentStartedAt, deadline, transactionId)
        requestQueue.offer(request)
        OrderPayer.logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
        logger.info("[$accountName] Submitted payment request into queue $paymentId, time spent ${now() - paymentStartedAt} ms")
    }

    private suspend fun processRequestQueue() {
        while (true) {
            try {
                val request = requestQueue.take()

                requestSemaphore.withPermit {
                    withContext(Dispatchers.IO) {
                        rateLimiter.tickBlocking()
                    }
                    processPaymentRequest(request)
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

        try {
            val httpRequest = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=${request.transactionId}&paymentId=${request.paymentId}&amount=${request.amount}")
                post(emptyBody)
            }.build()

            val response = withContext(Dispatchers.IO) {
                client.newCall(httpRequest).execute()
            }

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

                logger.info("[$accountName] Payment processed for txId: ${request.transactionId}, payment: ${request.paymentId}, succeeded: ${body.result}, message: ${body.message}, time spent ${now() - request.paymentStartedAt} ms")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                withContext(Dispatchers.IO) {
                    paymentESService.update(request.paymentId) {
                        it.logProcessing(body.result, now(), request.transactionId, reason = body.message)
                    }
                }
            }
        } catch (e: Exception) {
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