package epimetheus.prometheus.scrape

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.cache.affinity.AffinityKeyMapped
import org.apache.ignite.cache.query.annotations.QuerySqlField
import java.time.LocalDateTime
import java.time.ZoneId


data class ScrapeTargetKey(
        @QuerySqlField val jobName: String,
        @QuerySqlField @AffinityKeyMapped val target: String)

class ScrapeTarget(
        val url: String,
        val intervalSeconds: Float,
        val honorLabels: Boolean,
        val params: Map<String, List<String>>)

@JsonSerialize(using = ScrapeStatusSerializer::class)
interface ScrapeStatus
data class ScrapeStatusSuccess(val latencyNs: Long) : ScrapeStatus
data class ScrapeStatusFailure(val cause: Throwable) : ScrapeStatus

class ScrapeStatusSerializer: StdSerializer<ScrapeStatus>(ScrapeStatus::class.java) {
    override fun serialize(value: ScrapeStatus?, gen: JsonGenerator, provider: SerializerProvider?) {
        if (value == null) {
            gen.writeNull()
        }
        gen.writeStartObject()
        when (value) {
            is ScrapeStatusSuccess -> {
                gen.writeStringField("status", "ok")
                gen.writeNumberField("latencyNs", value.latencyNs)
            }
            is ScrapeStatusFailure -> {
                gen.writeStringField("status", "error")
                gen.writeStringField("reason", value.cause.message)
            }
        }
        gen.writeEndObject()
    }

}

data class ScrapeResult(val latencyNs: Long, val samples: List<ScrapedSample>)

@JsonSerialize(using = ScrapeScheduleSerializer::class)
data class ScrapeSchedule(
        val nextExec: LocalDateTime,
        val lastTimestamp: LocalDateTime?,
        val lastStatus: ScrapeStatus?
)

class ScrapeScheduleSerializer : StdSerializer<ScrapeSchedule>(ScrapeSchedule::class.java) {
    override fun serialize(value: ScrapeSchedule?, gen: JsonGenerator, provider: SerializerProvider?) {
        val zone = ZoneId.systemDefault()
        gen.writeStartObject()
        gen.writeObjectField("lastStatus", value?.lastStatus)
        gen.writeObjectField("nextExec", value?.nextExec?.atZone(zone)?.toInstant()?.toEpochMilli())
        gen.writeObjectField("lastTimestamp", value?.lastTimestamp?.atZone(zone)?.toInstant()?.toEpochMilli())
        gen.writeEndObject()
    }
}
