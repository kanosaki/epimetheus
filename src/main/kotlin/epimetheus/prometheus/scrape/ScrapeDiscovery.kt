package epimetheus.prometheus.scrape

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import epimetheus.pkg.serializer.DurationDeserializer
import epimetheus.pkg.serializer.DurationSerializer
import epimetheus.pkg.serializer.LocalDateTimeSerializer
import epimetheus.pkg.serializer.TimestampDeserializer
import epimetheus.prometheus.configfile.PrometheusGlobalConfig
import epimetheus.prometheus.configfile.ScrapeConfig
import java.time.Duration
import java.time.LocalDateTime

/**
 * @param refreshInterval Refreshing interval, null will be disable refreshing.
 */
class ScrapeDiscovery(
        val config: ScrapeConfig,

        @JsonSerialize(using = DurationSerializer::class)
        @JsonDeserialize(using = DurationDeserializer::class)
        val refreshInterval: Duration? = null,

        @JsonSerialize(using = LocalDateTimeSerializer::class)
        @JsonDeserialize(using = TimestampDeserializer::class)
        val lastRefresh: LocalDateTime? = null) {
    fun refreshTargets(global: PrometheusGlobalConfig): List<Pair<ScrapeTargetKey, ScrapeTarget>> {
        return config.materialize(global)
    }
}

