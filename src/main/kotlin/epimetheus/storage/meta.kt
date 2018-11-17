package epimetheus.storage

import epimetheus.CacheName.Prometheus.METRIC_META
import epimetheus.model.*
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.Ignite
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.cache.query.ScanQuery
import org.apache.ignite.cache.query.TextQuery
import org.apache.ignite.cache.query.annotations.QueryTextField
import org.apache.ignite.configuration.CacheConfiguration


typealias MetricKey = Signature

data class MetricInfo(@QueryTextField val name: String, val metric: Metric)

interface Meta : MetricRegistory {
    fun registerMetricsFromSamples(samples: Collection<ScrapedSample>)
    /**
     * Collects metrics with query, result must be sorted by its fingerprint
     */
    fun lookupMetrics(query: MetricMatcher): List<Metric>

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

    override fun registerMetricsFromSamples(samples: Collection<ScrapedSample>) {
        samples.forEach {
            val name = it.m[Metric.nameLabel]!!
            val met = Metric(it.m)
            metricMeta.putIfAbsent(met.fingerprint(), MetricInfo(name, met))
        }
    }

    override fun metric(id: Signature): Metric? {
        return metricMeta.get(id)?.metric
    }

    override fun lookupMetrics(query: MetricMatcher): List<Metric> {
        val name = query.namePattern()
        return if (name == null || name.lmt != LabelMatchType.Eq) {
            val mets = metricMeta
                    .query(ScanQuery<MetricKey, MetricInfo> { _, v -> query.matches(v.metric) }).toMutableList()
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

    }

    override fun close() {
        metricMeta.close()
    }
}
