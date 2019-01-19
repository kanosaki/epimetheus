package epimetheus.prometheus.scrape

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import epimetheus.pkg.serializer.LocalDateTimeSerializer
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.cache.affinity.AffinityKeyMapped
import org.apache.ignite.cache.query.annotations.QuerySqlField
import java.time.LocalDateTime


data class ScrapeTargetKey(
        @QuerySqlField val jobName: String,
        @QuerySqlField @AffinityKeyMapped val target: String)

data class ScrapeTarget(
        val url: String,
        val intervalSeconds: Float,
        val honorLabels: Boolean,
        val params: Map<String, List<String>>)

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "status"
)
@JsonSubTypes(
        JsonSubTypes.Type(value = ScrapeResultSuccess::class, name = "ok"),
        JsonSubTypes.Type(value = ScrapeResultFailure::class, name = "error")
)
interface ScrapeResult

@JsonTypeName("ok")
data class ScrapeResultSuccess(val latencyNs: Long) : ScrapeResult

@JsonTypeName("error")
data class ScrapeResultFailure(val cause: Throwable) : ScrapeResult

data class ScrapeResponse(val latencyNs: Long, val samples: List<ScrapedSample>)

data class ScrapeStatus(
        @JsonSerialize(using = LocalDateTimeSerializer::class)
        val nextExec: LocalDateTime,
        @JsonSerialize(using = LocalDateTimeSerializer::class)
        val lastTimestamp: LocalDateTime?,
        val lastResult: ScrapeResult?
)
