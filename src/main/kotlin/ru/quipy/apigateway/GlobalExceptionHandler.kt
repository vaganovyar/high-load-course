package ru.quipy.apigateway

import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import ru.quipy.payments.logic.TooManyRequestsException

@ControllerAdvice
class GlobalExceptionHandler {

    @ExceptionHandler(TooManyRequestsException::class)
    fun handleTooManyRequests(e: TooManyRequestsException): ResponseEntity<String> {
        val headers = HttpHeaders()

        e.retryAfterTimestamp?.let { timestamp ->
            headers.set("Retry-After", timestamp.toString())
        }

        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .headers(headers)
            .body("Too many requests: ${e.message}")
    }
}
