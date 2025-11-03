package ru.quipy.payments.logic

import java.util.*
import java.util.concurrent.LinkedBlockingQueue

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