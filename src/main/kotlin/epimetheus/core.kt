package epimetheus

import epimetheus.prometheus.configfile.ConfigFile
import epimetheus.prometheus.scrape.ScrapeTarget
import epimetheus.prometheus.scrape.ScrapeTargetKey
import org.apache.ignite.Ignite
import org.apache.ignite.configuration.CacheConfiguration

class EpimetheusCore(val ignite: Ignite) {
    private val config = ignite.getOrCreateCache(CacheConfiguration<String, Any>().apply {
        name = CacheName.CONFIG
        backups = 1
    })

    fun applyPrometheusConfig(v: ConfigFile) {
        // parse prometheus config
        val targets = v.scrapeConfig.flatMap { it.materialize(v.global) }
        val targetCache = ignite.cache<ScrapeTargetKey, ScrapeTarget>(CacheName.Prometheus.SCRAPE_TARGETS)
        targets.forEach { targetCache.put(it.first, it.second) }
    }
}