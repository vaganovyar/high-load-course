package ru.quipy.payments.logic

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

data class PaymentRequest(
    val paymentId: UUID,
    val orderId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long,
    val transactionId: UUID,
    val retries: Int = 0,
)

class PaymentQueue(
    private val maxRetries: Int = -1,
): LinkedBlockingQueue<PaymentRequest>() {
    override fun offer(e: PaymentRequest): Boolean {
        if (maxRetries > 0 && e.retries > maxRetries) {
            return false
        }
        return super.offer(e)
    }

    fun containsByPaymentId(paymentId: UUID) = this.any { it.paymentId == paymentId }
}

class PaymentChannel(
    private val maxRetries: Int = -1,
) {
    private val channel = Channel<PaymentRequest>(Channel.UNLIMITED)
    private val queueSize = AtomicInteger(0)

    suspend fun offer(e: PaymentRequest): Boolean {
        if (maxRetries > 0 && e.retries > maxRetries) {
            return false
        }
        channel.send(e)
        queueSize.incrementAndGet()
        return true
    }

    suspend fun take(): PaymentRequest {
        val request = channel.receive()
        queueSize.decrementAndGet()
        return request
    }

    fun size(): Int = queueSize.get()

}