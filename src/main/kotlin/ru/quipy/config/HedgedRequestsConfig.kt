package ru.quipy.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("hedged-requests")
data class HedgedRequestsConfig(
    var tries: Int = 1,
    var delay: Long = 500,
)