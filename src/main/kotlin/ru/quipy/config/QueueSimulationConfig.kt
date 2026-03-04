package ru.quipy.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("queue-simulation")
data class QueueSimulationConfig(
    var enabled: Boolean = false
)