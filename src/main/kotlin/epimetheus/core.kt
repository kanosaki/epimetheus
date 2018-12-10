package epimetheus

import epimetheus.prometheus.ConfigFile
import epimetheus.prometheus.scrape.ScrapeTarget
import epimetheus.prometheus.scrape.ScrapeTargetKey
import org.apache.ignite.Ignite

class EpimetheusCore(val ignite: Ignite) {
    fun applyPrometheusConfig(v: ConfigFile) {
        // parse prometheus config
        val targets = v.scrapeConfig.flatMap { it.materialize(v.global) }
        val targetCache = ignite.cache<ScrapeTargetKey, ScrapeTarget>(CacheName.Prometheus.SCRAPE_TARGETS)
        targets.forEach { targetCache.put(it.first, it.second) }
    }
}