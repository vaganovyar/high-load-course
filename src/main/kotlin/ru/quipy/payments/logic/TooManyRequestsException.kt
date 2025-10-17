package ru.quipy.payments.logic

/**
 * Exception thrown when too many requests are received and back pressure is applied.
 * This should result in HTTP 429 Too Many Requests response.
 */
class TooManyRequestsException(
    message: String,
    val retryAfterTimestamp: Long? = null
) : RuntimeException(message)
