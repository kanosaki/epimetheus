package epimetheus.storage

import epimetheus.CacheName.Prometheus.FRESH_SAMPLES
import epimetheus.model.Metric
import epimetheus.model.Series
import epimetheus.model.TimeFrames
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.Ignite
import org.apache.ignite.cache.affinity.AffinityKeyMapped
import org.apache.ignite.cache.query.SqlQuery
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.configuration.CacheConfiguration


data class FreshSampleKey(
        @AffinityKeyMapped @QuerySqlField(notNull = true) val instance: String,
        @QuerySqlField(index = true, notNull = true) val metricID: Long,
        @QuerySqlField(index = true, notNull = true) val timestamp: Long)

data class FreshSample(val value: Double)


/**
 * FreshStorage is write intensive area. Affinity is defined by "instance" (in prometheus term)
 * to achieve better scalability (though it assumes even scrape_interval distribution.)
 * And here, capacity efficiency is compromised for write performance.
 */
class Fresh(val ignite: Ignite) {
    private val freshCache = ignite.getOrCreateCache(CacheConfiguration<FreshSampleKey, FreshSample>().apply {
        name = FRESH_SAMPLES
        backups = 1
        setIndexedTypes(FreshSampleKey::class.java, FreshSample::class.java)
    })

    fun push(instance: String, ts: Long, samples: Collection<ScrapedSample>) {
        samples.forEach { s ->
            val metricID = Metric.labelsFingerprintFNV(s.m)
            freshCache.put(FreshSampleKey(instance, metricID, ts), FreshSample(s.value))
        }
    }

    fun collect(metric: Metric, range: TimeFrames): Series {
        val instance = metric.m[Metric.instanceLabel]
        val metricID = metric.fingerprint()
        val cur = if (instance == null) {
            // without instance field (query will be executed all nodes in the cluster)
            val query = "SELECT * FROM FreshSample WHERE metricID = ? AND ? <= timestamp AND timestamp < ? ORDER BY timestamp DESC"
            freshCache.query(
                    SqlQuery<FreshSampleKey, FreshSample>(FreshSample::class.java, query)
                            .setArgs(metricID, range.start, range.end)
            ).all
        } else {
            // with instance field (query will be executed a single node in the cluster): preferred way
            val query = "SELECT * FROM FreshSample WHERE instance = ? AND metricID = ? AND ? <= timestamp AND timestamp < ? ORDER BY timestamp DESC"
            freshCache.query(
                    SqlQuery<FreshSampleKey, FreshSample>(FreshSample::class.java, query)
                            .setArgs(instance, metricID, range.start, range.end)
            ).all

        }
        val timestamps = LongArray(cur.size) { cur[it].key.timestamp }
        val values = DoubleArray(cur.size) { cur[it].value.value }
        return Series(metric, values, timestamps)
    }
}
