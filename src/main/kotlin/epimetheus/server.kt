package epimetheus

import epimetheus.ServiceName.Prometheus.API_SERVER
import epimetheus.ServiceName.Prometheus.SCRAPE_SERVICE
import epimetheus.prometheus.ConfigFile
import epimetheus.prometheus.IgniteAPI
import epimetheus.prometheus.Parser
import epimetheus.prometheus.scrape.ScrapeService
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.services.ServiceConfiguration
import java.io.File


class EpimetheusServer(igniteConfig: IgniteConfiguration) : AutoCloseable {
    private val ignite = Ignition.start(igniteConfig)
    private val core = EpimetheusCore(ignite)

    override fun close() {
        ignite.close()
    }

    fun applyLocalPrometheusConfig(file: File) {
        // parse prometheus config
        val v = Parser.mapper.readValue<ConfigFile>(file, ConfigFile::class.java)
        core.applyPrometheusConfig(v)
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
