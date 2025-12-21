package ru.quipy.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("back-pressure")
data class BackPressureConfig(
    var usePreciseQueueProcessTime: Boolean = false
)

