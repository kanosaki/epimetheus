package epimetheus

import epimetheus.prometheus.configfile.ConfigFile
import epimetheus.prometheus.scrape.ScrapeDiscovery
import epimetheus.prometheus.scrape.ScrapeGateway
import epimetheus.prometheus.scrape.ScrapeTarget
import epimetheus.prometheus.scrape.ScrapeTargetKey
import org.apache.ignite.Ignite
import org.apache.ignite.configuration.CacheConfiguration

class EpimetheusCore(val ignite: Ignite) {
    val config = ClusterConfig(ignite)

    fun applyPrometheusConfig(v: ConfigFile) {
        config.prometheusGlobal = v.global

        // parse prometheus config
        val gate = ScrapeGateway(ignite)
        v.scrapeConfig.forEach {
            gate.putDiscovery(it.name, ScrapeDiscovery(it))
        }
    }
}