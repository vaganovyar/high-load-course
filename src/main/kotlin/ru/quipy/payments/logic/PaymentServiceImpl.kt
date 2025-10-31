package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(orderId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var bestRetryAfterTimestamp: Long? = null
        
        for (account in paymentAccounts) {
            try {
                account.performPaymentAsync(paymentId, orderId, amount, paymentStartedAt, deadline)
                return
            } catch (e: TooManyRequestsException) {
                // Keep the earliest retry timestamp
                e.retryAfterTimestamp?.let { timestamp ->
                    if (bestRetryAfterTimestamp == null || timestamp < bestRetryAfterTimestamp!!) {
                        bestRetryAfterTimestamp = timestamp
                    }
                }
            }
        }
        throw TooManyRequestsException("All payment accounts are under back pressure", bestRetryAfterTimestamp)
    }
}