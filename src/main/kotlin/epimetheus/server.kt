package epimetheus

import epimetheus.ServiceName.Prometheus.API_SERVER
import epimetheus.ServiceName.Prometheus.SCRAPE_SERVICE
import epimetheus.prometheus.ConfigFile
import epimetheus.prometheus.IgniteAPI
import epimetheus.prometheus.Parser
import epimetheus.prometheus.scrape.ScrapeService
import epimetheus.prometheus.scrape.ScrapeTarget
import epimetheus.prometheus.scrape.ScrapeTargetKey
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.services.ServiceConfiguration
import java.io.File


class EpimetheusServer(igniteConfig: IgniteConfiguration) : AutoCloseable {
    private val ignite = Ignition.start(igniteConfig)

    override fun close() {
        ignite.close()
    }

    // TODO: provide prometheus config loader instead of giving a File directory, to achieve rule file and local file discovery config.
    fun applyLocalPrometheusConfig(file: File) {
        // parse prometheus config
        val v= Parser.mapper.readValue<ConfigFile>(file, ConfigFile::class.java)
        val targets = v.scrapeConfig.flatMap { it.materialize(v.global) }
        val targetCache = ignite.cache<ScrapeTargetKey, ScrapeTarget>(CacheName.Prometheus.SCRAPE_TARGETS)
        targets.forEach { targetCache.put(it.first, it.second) }
    }

    fun boot() {
        ignite.services().deploy(
                ServiceConfiguration().apply {
                    name = API_SERVER
                    service = IgniteAPI()
                    maxPerNodeCount = 1
                }
        )
        ignite.services().deploy(
                ServiceConfiguration().apply {
                    name = SCRAPE_SERVICE
                    service = ScrapeService()
                    maxPerNodeCount = 1
                }
        )
    }
}
