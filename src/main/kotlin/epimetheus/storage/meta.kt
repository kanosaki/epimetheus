package epimetheus.storage

import com.google.common.cache.CacheBuilder
import epimetheus.CacheName.Prometheus.METRIC_META
import epimetheus.model.*
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.Ignite
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.query.ScanQuery
import org.apache.ignite.cache.query.TextQuery
import org.apache.ignite.cache.query.annotations.QueryTextField
import org.apache.ignite.configuration.CacheConfiguration
import java.time.Duration


typealias MetricKey = Signature

data class MetricInfo(@QueryTextField val name: String, val metric: Metric)

interface Meta : MetricRegistry {
    fun registerMetricsFromSamples(samples: Collection<ScrapedSample>)
    /**
     * Collects metrics with query, result must be sorted by its fingerprint
     */

    fun metricIDs(query: MetricMatcher): List<Signature> {
        return lookupMetrics(query).map { it.fingerprint() }
    }
}

class IgniteMeta(val ignite: Ignite) : Meta, AutoCloseable {
    val metricMeta = ignite.getOrCreateCache(CacheConfiguration<MetricKey, MetricInfo>().apply {
        name = METRIC_META
        cacheMode = CacheMode.REPLICATED
        setIndexedTypes(MetricKey::class.java, MetricInfo::class.java)
    })
    private val metCache = CacheBuilder
            .newBuilder()
            .concurrencyLevel(10)
            .expireAfterAccess(Duration.ofMinutes(1))
            .build<Signature, Boolean>()

    private val matchCache = CacheBuilder
            .newBuilder()
            .concurrencyLevel(50)
            .maximumWeight(10000) // make this configurable
            .weigher<MetricMatcher, List<Metric>> { _, value -> value.size }
            .expireAfterWrite(Duration.ofMinutes(5))
            .build<MetricMatcher, List<Metric>>()

    override fun registerMetricsFromSamples(samples: Collection<ScrapedSample>) {
        samples.forEach {
            val name = it.met.name() ?: ""
            val met = it.met
            val cached = metCache.getIfPresent(met.fingerprint())
            if (cached == null) {
                metCache.put(met.fingerprint(), true)
                metricMeta.putIfAbsent(met.fingerprint(), MetricInfo(name, met))
            }
        }
    }

    override fun metric(id: Signature): Metric? {
        return metricMeta.get(id)?.metric
    }

    override fun lookupMetrics(query: MetricMatcher): List<Metric> {
        val c = matchCache.getIfPresent(query)
        if (c != null) {
            return c
        }
        val name = query.namePattern()
        val ret = if (name == null || name.lmt != LabelMatchType.Eq) {
            val mets = metricMeta
                    .query(ScanQuery<MetricKey, MetricInfo> { _, v -> query.matches(v.metric) })
                    .toMutableList()
            mets.sortBy { it.key }
            mets.map { it.value.metric }
        } else {
            val mets = metricMeta
                    .query(TextQuery<MetricKey, MetricInfo>(MetricInfo::class.java, name.value))
                    .filter { query.matches(it.value.metric, true) }
                    .toMutableList()
            mets.sortBy { it.key }
            mets.map { it.value.metric }
        }
        matchCache.put(query, ret)
        return ret
    }

    override fun close() {
        metricMeta.close()
    }
}
