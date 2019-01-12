package epimetheus.prometheus.scrape

import epimetheus.CacheName
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheInterceptorAdapter
import org.apache.ignite.cache.CachePeekMode
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.resources.IgniteInstanceResource
import java.time.LocalDateTime
import javax.cache.Cache

class ScrapeGateway(val ignite: Ignite) {
    val statusConf: CacheConfiguration<ScrapeTargetKey, ScrapeSchedule>
    val statuses: IgniteCache<ScrapeTargetKey, ScrapeSchedule>
    val targetConf: CacheConfiguration<ScrapeTargetKey, ScrapeTarget>
    val targets: IgniteCache<ScrapeTargetKey, ScrapeTarget>

    init {
        statusConf = CacheConfiguration<ScrapeTargetKey, ScrapeSchedule>().apply {
            name = CacheName.Prometheus.SCRAPE_STATUSES
            backups = 1
            interceptor = StatusCacheInterceptor()
        }
        statuses = ignite.getOrCreateCache<ScrapeTargetKey, ScrapeSchedule>(statusConf)

        targetConf = CacheConfiguration<ScrapeTargetKey, ScrapeTarget>().apply {
            name = CacheName.Prometheus.SCRAPE_TARGETS
            backups = 1
            interceptor = TargetCacheInterceptor()
        }
        targets = ignite.getOrCreateCache<ScrapeTargetKey, ScrapeTarget>(targetConf)
    }

    fun target(key: ScrapeTargetKey): ScrapeTarget {
        return targets.get(key)
    }

    fun updateStatus(key: ScrapeTargetKey, sched: ScrapeSchedule) {
        statuses.put(key, sched)
    }

    fun localStatuses(): Iterable<Cache.Entry<ScrapeTargetKey, ScrapeSchedule>> {
        return statuses.localEntries(CachePeekMode.PRIMARY)
    }
}

class StatusCacheInterceptor : CacheInterceptorAdapter<ScrapeTargetKey, ScrapeSchedule>() {
    // TODO: notify?
    //override fun onBeforePut(entry: Cache.Entry<ScrapeTargetKey, ScrapeSchedule>?, newVal: ScrapeSchedule?): ScrapeSchedule? {
    //    if (newVal == null) return newVal
    //    svc.schedule(LocalDateTime.now(), entry!!.key, newVal)
    //    return newVal
    //}
}

class TargetCacheInterceptor : CacheInterceptorAdapter<ScrapeTargetKey, ScrapeTarget>() {
    @IgniteInstanceResource
    @Transient
    private lateinit var ignite: Ignite

    @Transient
    private var statuses: IgniteCache<ScrapeTargetKey, ScrapeSchedule>? = null

    private fun checkStatusCache() {
        if (statuses == null) {
            statuses = ignite.cache(CacheName.Prometheus.SCRAPE_STATUSES)
        }
    }

    override fun onAfterPut(entry: Cache.Entry<ScrapeTargetKey, ScrapeTarget>?) {
        // use async! to prevent striped executor blocked
        checkStatusCache()
        Ignition.ignite().executorService().submit {
            statuses!!.put(entry!!.key, ScrapeSchedule(LocalDateTime.now(), null, null))
        }
    }

    override fun onAfterRemove(entry: Cache.Entry<ScrapeTargetKey, ScrapeTarget>?) {
        checkStatusCache()
        Ignition.ignite().executorService().submit {
            statuses!!.remove(entry!!.key) // cascade delete, use async!
        }
    }
}
