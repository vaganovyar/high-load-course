package ru.quipy.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("thread-pools")
data class ThreadPoolsConfig(
    var paymentExternalServiceThreadPoolConfig: ThreadPoolProperties = ThreadPoolProperties(),
    var parallelIdempotencyRequestsExecutorServiceThreadPoolConfig: ParallelIdempotencyRequestsExecutorServiceThreadPoolConfig = ParallelIdempotencyRequestsExecutorServiceThreadPoolConfig(),
) {
    data class ThreadPoolProperties(
        var threadsNumber: Int = 1,
        var backgroundTaskThreadsNumber: Int = 1,
        var requestHandlerThreadsNumber: Int = 32
    )

    data class ParallelIdempotencyRequestsExecutorServiceThreadPoolConfig(
        var threadsNumber: Int = 1,
    )
}