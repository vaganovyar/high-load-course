package ru.quipy.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("thread-pools")
data class ThreadPoolsConfig(
    var paymentExternalServiceThreadPoolConfig: ThreadPoolProperties = ThreadPoolProperties()
) {
    data class ThreadPoolProperties(
        var threadsNumber: Int = 1,
    )
}