package ru.quipy.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("retries")
class RetriesConfig(
    var payment: MaxRetryConfig = MaxRetryConfig()
) {
    data class MaxRetryConfig(
        var maxRetries: Int = -1,
    )
}