package ru.quipy.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("metrics")
class MetricsConfig(
    var incomingRequests: CounterProperties = CounterProperties(),
    var outgoingResponses: CounterProperties = CounterProperties(),
    var completedTasks: CounterProperties = CounterProperties(),
    var queueSize: GaugeProperties = GaugeProperties(),
    var externalSystemResponseTime: TimerProperties = TimerProperties(),
    var externalSystemSinglePaymentRetries: CounterProperties = CounterProperties(),
) {
    open class CommonMetricProperties(
        var name: String = "defaultName",
        var description: String = "defaultDescription",
    )

    data class CounterProperties(
        var tags: List<String> = emptyList(),
    ): CommonMetricProperties()

    class GaugeProperties: CommonMetricProperties()

    data class TimerProperties(
        var quantiles: List<Double> = emptyList(),
    ): CommonMetricProperties()
}