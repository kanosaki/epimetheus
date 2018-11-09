package epimetheus.prometheus.scrape

import epimetheus.CacheName.Prometheus.SCRAPE_STATUSES
import epimetheus.CacheName.Prometheus.SCRAPE_TARGETS
import epimetheus.ScrapeSchedule
import org.apache.ignite.Ignite
import java.time.LocalDateTime

class ScrapeStatusCache(private val ignite: Ignite) {
    private val statuses = ignite.cache<ScrapeTargetName, ScrapeSchedule>(SCRAPE_STATUSES)
    private val targets = ignite.cache<ScrapeTargetName, ScrapeTarget>(SCRAPE_TARGETS)

    fun update(sched: ScrapeSchedule) {
        statuses.put(sched.key, sched)
    }

    fun restoreStatuses(): Iterable<ScrapeSchedule> {
        val now = LocalDateTime.now()
        return targets.localEntries().map {
            statuses.get(it.key) ?: ScrapeSchedule(it.key, now, null, null)
        }
    }
}