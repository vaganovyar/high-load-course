package ru.quipy.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("metrics")
class MetricsConfig(
    var incomingRequests: MetricProperties = MetricProperties(),
    var outgoingResponses: MetricProperties = MetricProperties(),
    var completedTasks: MetricProperties = MetricProperties(),
    var queueSize: MetricProperties = MetricProperties(),
) {
    data class MetricProperties(
        var name: String = "defaultName",
        var description: String = "defaultDescription",
        var tags: List<String> = emptyList(),
    )
}