package ru.quipy.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("logging")
class LogConfig(
    var enabled: Boolean = true,
    var sample: Int = 1,
) {
}