package ru.quipy.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import org.springframework.stereotype.Service
import ru.quipy.config.MetricsConfig

@Service
class MetricsService(
    val metricsConfig: MetricsConfig,
) {
    fun <T> runWithMetric(tags: List<String>, block: () -> T): T {
        if (tags.size != metricsConfig.incomingRequests.tags.size) {
            throw RuntimeException("Incorrect size of provided tags")
        }
        registerCounter(metricsConfig.incomingRequests, tags).increment()
        val response = block()
        registerCounter(metricsConfig.outgoingResponses, tags).increment()
        return response
    }

    fun incrementCompletedTask(method: String) {
        registerCounter(metricsConfig.completedTasks, listOf(method)).increment()
    }

    fun registerQueueSizeGauge(accountName: String, gaugeObject: Any, valueFunction: () -> Double) {
        val tags = listOf(Tag.of("account", accountName))
        Gauge.builder(metricsConfig.queueSize.name, gaugeObject) { valueFunction() }
            .description(metricsConfig.queueSize.description)
            .tags(tags)
            .register(Metrics.globalRegistry)
    }

    private fun registerCounter(config: MetricsConfig.MetricProperties, tags: List<String>): Counter {
        val counterTags: List<Tag> =
            config.tags.mapIndexed { index, element ->
                Tag.of(element, tags[index])
            }
        return Counter
            .builder(config.name)
            .tags(counterTags)
            .description(config.description)
            .register(Metrics.globalRegistry)
    }
}