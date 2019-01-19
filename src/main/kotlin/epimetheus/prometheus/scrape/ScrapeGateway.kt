package epimetheus.prometheus.scrape

import epimetheus.CacheName
import epimetheus.ClusterConfig
import epimetheus.transaction
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CachePeekMode
import org.apache.ignite.cache.query.ScanQuery
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.lang.IgniteBiPredicate
import java.time.LocalDateTime
import javax.cache.Cache

class ScrapeGateway(val ignite: Ignite) {
    private val igniteTx = ignite.transactions()
    private val config = ClusterConfig(ignite)

    private val statusConf: CacheConfiguration<ScrapeTargetKey, ScrapeStatus>
    val statuses: IgniteCache<ScrapeTargetKey, ScrapeStatus>
    private val targetConf: CacheConfiguration<ScrapeTargetKey, ScrapeTarget>
    val targets: IgniteCache<ScrapeTargetKey, ScrapeTarget>

    private val discoveryConf: CacheConfiguration<String, ScrapeDiscovery>
    val discoveries: IgniteCache<String, ScrapeDiscovery>

    init {
        statusConf = CacheConfiguration<ScrapeTargetKey, ScrapeStatus>().apply {
            name = CacheName.Prometheus.SCRAPE_STATUSES
            atomicityMode = CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT
            backups = 1
        }
        statuses = ignite.getOrCreateCache<ScrapeTargetKey, ScrapeStatus>(statusConf)

        targetConf = CacheConfiguration<ScrapeTargetKey, ScrapeTarget>().apply {
            name = CacheName.Prometheus.SCRAPE_TARGETS
            atomicityMode = CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT
            backups = 1
        }
        targets = ignite.getOrCreateCache<ScrapeTargetKey, ScrapeTarget>(targetConf)

        discoveryConf = CacheConfiguration<String, ScrapeDiscovery>().apply {
            name = CacheName.Prometheus.SCRAPE_DISCOVERIES
            atomicityMode = CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT
            backups = 1
        }
        discoveries = ignite.getOrCreateCache(discoveryConf)
    }

    fun target(key: ScrapeTargetKey): ScrapeTarget {
        return targets.get(key)
    }

    fun updateStatus(key: ScrapeTargetKey, status: ScrapeStatus) {
        // TODO: check existence of target entry?
        statuses.put(key, status)
    }

    fun putTarget(key: ScrapeTargetKey, target: ScrapeTarget) {
        transaction(igniteTx) {
            val prev = targets.get(key)
            if (prev == null || prev != target) {
                targets.put(key, target)
                // clear status
                statuses.put(key, ScrapeStatus(LocalDateTime.now(), null, null))
            }
            true
        }
    }

    fun putDiscovery(name: String, discovery: ScrapeDiscovery) {
        val global = config.prometheusGlobal
        transaction(igniteTx) { tx ->
            // delete old targets
            val prevTargets = targets.query(ScanQuery<ScrapeTargetKey, ScrapeTarget>(IgniteBiPredicate { k, v ->
                k.jobName == name
            }))

            targets.removeAll(prevTargets.map { it.key }.toSet())
            discoveries.put(name, discovery)

            val targets = discovery.refreshTargets(global)
            targets.forEach {
                assert(it.first.jobName == name)
                putTarget(it.first, it.second)
            }
            true
        }
    }

    fun nodeAssignedTargets(): Iterable<Cache.Entry<ScrapeTargetKey, ScrapeTarget>> {
        return targets.localEntries(CachePeekMode.PRIMARY)
    }
}
