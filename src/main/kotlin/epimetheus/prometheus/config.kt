package epimetheus.prometheus

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.ignite.cache.query.annotations.QuerySqlField
import java.time.Duration

object Parser {
    val mapper = ObjectMapper(YAMLFactory())

    init {
        mapper.registerModule(KotlinModule())
    }
}


class DurationDeserializer() : JsonDeserializer<Duration?>() {
    override fun deserialize(p: JsonParser?, ctxt: DeserializationContext?): Duration? {
        if (p == null) return null
        val txt = p.text
        when {
            txt.endsWith("s") -> {
                val v = txt.removeSuffix("s")
                return Duration.ofMillis((v.toDouble() * 1000).toLong())
            }
            txt.endsWith("m") -> {
                val v = txt.removeSuffix("m")
                return Duration.ofMillis((v.toDouble() * 1000 * 60).toLong())

            }
            txt.endsWith("h") -> {
                val v = txt.removeSuffix("h")
                return Duration.ofMillis((v.toDouble() * 1000 * 60 * 60).toLong())
            }
        }
        // TODO: treat as seconds?
        return Duration.ofMillis((txt.toDouble() * 1000).toLong())
    }
}

data class Config(
        @JsonProperty("scrape_configs")
        val scrapeConfig: List<ScrapeConfig>,
        @JsonProperty("global")
        val global: Global
)

data class Global(
        @JsonDeserialize(using = DurationDeserializer::class)
        @JsonProperty("scrape_interval") val scrapeInterval: Duration
)


data class ScrapeConfig(
        @JsonProperty("job_name", required = true)
        val name: String,

        @JsonDeserialize(using = DurationDeserializer::class)
        @JsonProperty("scrape_interval")
        val scrapeInterval: Duration?,

        @JsonProperty("metrics_path")
        val metricsPath: String?,

        @JsonProperty("honor_labels")
        val honorLabels: Boolean?,

        @JsonProperty("scheme")
        val scheme: String?,

        @JsonProperty("args")
        val params: Map<String, List<String>>?,

        @JsonProperty("static_configs")
        val staticConfigs: List<StaticConfig>?
) {
    fun fullfill(global: Global): ScrapeConfig {
        return ScrapeConfig(
                name,
                scrapeInterval ?: global.scrapeInterval,
                metricsPath ?: "/metrics",
                honorLabels ?: false,
                scheme ?: "http",
                params ?: mapOf(),
                staticConfigs ?: listOf()
        )
    }
}

data class StaticConfig(val targets: List<String>, val labels: Map<String, String>)

data class APIServerConfiguration(val port: Int)

