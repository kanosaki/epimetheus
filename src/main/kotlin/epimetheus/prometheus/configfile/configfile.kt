package epimetheus.prometheus.configfile

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import epimetheus.EpimetheusException
import epimetheus.pkg.serializer.DurationSerializer
import epimetheus.pkg.serializer.DurationDeserializer
import epimetheus.prometheus.scrape.ScrapeTarget
import epimetheus.prometheus.scrape.ScrapeTargetKey
import org.apache.http.client.utils.URIBuilder
import org.apache.http.message.BasicNameValuePair
import java.time.Duration

object Parser {
    val mapper = ObjectMapper(YAMLFactory())

    init {
        mapper.registerModule(KotlinModule())
    }
}


data class ConfigFile(
        @JsonProperty("scrape_configs")
        val scrapeConfig: List<ScrapeConfig>,
        @JsonProperty("global")
        val global: PrometheusGlobalConfig
)

data class PrometheusGlobalConfig(
        @JsonDeserialize(using = DurationDeserializer::class)
        @JsonSerialize(using = DurationSerializer::class)
        @JsonProperty("scrape_interval") val scrapeInterval: Duration
)

data class ScrapeConfig(
        @JsonProperty("job_name", required = true)
        val name: String,

        @JsonDeserialize(using = DurationDeserializer::class)
        @JsonSerialize(using = DurationSerializer::class)
        @JsonProperty("scrape_interval")
        val scrapeInterval: Duration?,

        @JsonProperty("metrics_path")
        val metricsPath: String?,

        @JsonProperty("honor_labels")
        val honorLabels: Boolean?,

        @JsonProperty("scheme")
        val scheme: String?,

        @JsonProperty("params")
        val params: Map<String, List<String>>?,

        @JsonProperty("static_configs")
        val staticConfigs: List<StaticConfig>?
) {
    fun materialize(global: PrometheusGlobalConfig): List<Pair<ScrapeTargetKey, ScrapeTarget>> {
        return staticConfigs?.flatMap { it.materialize(global, this) } ?: listOf()
    }

    fun scrapeIntervalSeconds(global: PrometheusGlobalConfig): Float {
        return (this.scrapeInterval ?: global.scrapeInterval).seconds.toFloat()
    }

    fun baseUriBuilder(): URIBuilder {
        val ub = URIBuilder()
        ub.scheme = this.scheme ?: "http"
        ub.path = this.metricsPath ?: "/metrics"
        ub.setParameters(params?.entries?.flatMap { param ->
            param.value.map { BasicNameValuePair(param.key, it) }
        })
        return ub
    }
}

abstract class ScrapeTargetFinder {
    abstract fun materialize(global: PrometheusGlobalConfig, sc: ScrapeConfig): List<Pair<ScrapeTargetKey, ScrapeTarget>>

    protected fun splitHostPort(target: String): Pair<String, Int> {
        val hostport = target.split(':', limit = 2)
        if (hostport.isEmpty()) {
            throw EpimetheusException("static_config target format error")
        }
        return hostport[0] to (hostport.getOrElse(1) { "80" }).toInt()
    }
}

data class StaticConfig(@JsonProperty("targets") val targets: List<String>, @JsonProperty("labels") val labels: Map<String, String>?) : ScrapeTargetFinder() {
    override fun materialize(global: PrometheusGlobalConfig, sc: ScrapeConfig): List<Pair<ScrapeTargetKey, ScrapeTarget>> {
        return targets.map { target ->
            val hostport = splitHostPort(target)
            val uri = sc.baseUriBuilder().apply {
                host = hostport.first
                port = hostport.second
            }
            val lo = if (sc.honorLabels == true) "" else {
                val entries = labels?.entries?.map { it.key to it.value }.orEmpty() + listOf("instance" to target, "job" to sc.name)
                entries.joinToString(",") { """${it.first}="${it.second}"""" }
            }
            ScrapeTargetKey(sc.name, target) to ScrapeTarget(uri.toString(), sc.scrapeIntervalSeconds(global), sc.honorLabels
                    ?: false, sc.params ?: mapOf())
        }
    }
}

data class APIServerConfig(val port: Int, val workers: Int, val corsOrigin: String = "*")

