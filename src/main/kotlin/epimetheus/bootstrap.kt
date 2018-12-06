package epimetheus

import epimetheus.CacheName.Prometheus.SCRAPE_TARGETS
import epimetheus.ServiceName.Prometheus.API_SERVER
import epimetheus.ServiceName.Prometheus.SCRAPE_DISPATCHER
import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.prometheus.Config
import epimetheus.prometheus.IgniteAPI
import epimetheus.prometheus.scrape.ScrapeDispatcher
import epimetheus.prometheus.scrape.ScrapeTarget
import epimetheus.prometheus.scrape.ScrapeTargetName
import org.apache.ignite.Ignite
import org.apache.ignite.services.ServiceConfiguration
import java.time.LocalDateTime

interface ScrapeStatus
data class ScrapeStatusSuccess(val latencyNs: Long) : ScrapeStatus
data class ScrapeStatusFailure(val cause: Throwable) : ScrapeStatus

data class ScrapeResult(val latencyNs: Long, val samples: List<ScrapedSample>)

data class ScrapeSchedule(
        val key: ScrapeTargetName,
        val nextExec: LocalDateTime,
        val lastTimestamp: LocalDateTime?,
        val lastStatus: ScrapeStatus?
)

class Bootstrap(val config: Config, val ignite: Ignite) {
    fun run() {
        this.updateConfig() // TODO: make this optional
        this.startApis()
        this.startScrapers()
        while (true) {
            Thread.sleep(2000)
        }
    }

    fun updateConfig() {
        val confCache = ignite.cache<ScrapeTargetName, ScrapeTarget>(SCRAPE_TARGETS)
        config.scrapeConfig.forEach { sc ->
            val filled = sc.fullfill(config.global)
            filled.staticConfigs?.forEach { s ->
                s.targets.forEach { target ->
                    confCache.put(
                            ScrapeTargetName(filled.name, target),
                            ScrapeTarget(
                                    sc.name,
                                    "${filled.scheme}://$target${filled.metricsPath}",
                                    filled.scrapeInterval!!.nano.toFloat() * 1e-9f,
                                    filled.honorLabels!!,
                                    filled.params!!))
                }
            }
        }
    }

    fun startApis() {
        ignite.services().deploy(
                ServiceConfiguration().apply {
                    name = API_SERVER
                    service = IgniteAPI()
                    maxPerNodeCount = 1
                }
        )
    }

    fun startScrapers() {
        ignite.services().deploy(
                ServiceConfiguration().apply {
                    name = SCRAPE_DISPATCHER
                    service = ScrapeDispatcher()
                    maxPerNodeCount = 1
                }
        )
    }
}
