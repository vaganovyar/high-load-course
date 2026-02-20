package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import java.time.Duration

class TimeToProcessCounter(
    public val requestAverageProcessingTime: Duration,
    public val maxConcurrentRequests: Int,
    public val rateLimitPerSec: Int,
) {
    companion object {
        val logger = LoggerFactory.getLogger(TimeToProcessCounter::class.java)

        private const val precomputeLimit = 1_000_000
    }

    private val observations = ArrayList<Observation>()


    init {
        var processedRequests = 0
        var parallelRequests = 0
        var requestsInPrevSecond = 0
        var currentTime = Duration.ZERO
        var rpsDeque = ArrayDeque<Observation>()
        var parallelDeque = ArrayDeque<Observation>()

        while (processedRequests < precomputeLimit) {
            while (rpsDeque.size > 0 && currentTime - rpsDeque.first().time > Duration.ofSeconds(1)) {
                requestsInPrevSecond -= rpsDeque.removeFirst().requestsCount
            }
            while (parallelDeque.size > 0 && currentTime - parallelDeque.first().time > requestAverageProcessingTime) {
                parallelRequests -= parallelDeque.removeFirst().requestsCount
            }
            if (requestsInPrevSecond == rateLimitPerSec) {
                currentTime = rpsDeque.first().time + Duration.ofSeconds(1) + Duration.ofMillis(1)
                continue
            }
            if (parallelRequests == maxConcurrentRequests) {
                currentTime = parallelDeque.first().time + requestAverageProcessingTime + Duration.ofMillis(1)
                continue
            }
            val toSend = minOf(maxConcurrentRequests - parallelRequests, rateLimitPerSec - requestsInPrevSecond)
            processedRequests += toSend
            parallelRequests += toSend
            requestsInPrevSecond += toSend
            observations.add(Observation(processedRequests, currentTime))
            rpsDeque.add(Observation(toSend, currentTime))
            parallelDeque.add(Observation(toSend, currentTime))
        }
        logger.info("Precomputed processing times for requestAverageProcessingTime=$requestAverageProcessingTime, maxConcurrentRequests=$maxConcurrentRequests, rateLimitPerSec=$rateLimitPerSec, first values = [${observations[0]}, ${observations[1]}, ${observations[2]}, ${observations[3]}, ${observations[4]}]")
    }

    public fun computeTimeToProcessQueue(queueSize: Int): Duration {
        val index = observations.binarySearch { it.requestsCount.compareTo(queueSize) }
        val sendTime = if (index >= 0) observations[index].time else observations[-index - 1].time
        return sendTime + requestAverageProcessingTime
    }

    data class Observation(
        val requestsCount: Int,
        val time: Duration,
    )
}