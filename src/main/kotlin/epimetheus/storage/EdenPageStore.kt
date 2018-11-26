package epimetheus.storage

import epimetheus.CacheName.Prometheus.FRESH_SAMPLES
import epimetheus.model.Mat
import epimetheus.model.Metric
import epimetheus.model.Series
import epimetheus.model.TimeFrames
import epimetheus.pkg.textparse.ScrapedSample
import it.unimi.dsi.fastutil.longs.Long2DoubleRBTreeMap
import it.unimi.dsi.fastutil.longs.LongArrayList
import org.apache.ignite.Ignite
import org.apache.ignite.cache.affinity.AffinityKeyMapped
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.stream.StreamTransformer
import java.util.*


data class EdenPageKey(
        @AffinityKeyMapped @QuerySqlField(notNull = true) val instance: String,
        @QuerySqlField(notNull = true) val metricID: Long,
        @QuerySqlField(notNull = true) val timestamp: Long)

data class EdenPage(val values: DoubleArray, val timestamps: LongArray) {
}

/**
 * FreshStorage is write intensive area. Affinity is defined by "instance" (in prometheus term)
 * to achieve better scalability (though it assumes even scrape_interval distribution.)
 * And here, capacity efficiency is compromised for write performance.
 */
class EdenPageStore(val ignite: Ignite, val windowSize: Long = 5 * 60 * 1000) : AutoCloseable {
    private val cache = ignite.getOrCreateCache(CacheConfiguration<EdenPageKey, EdenPage>().apply {
        name = FRESH_SAMPLES
        backups = 1
        //setIndexedTypes(EdenPageKey::class.java, EdenPage::class.java)
    })

    fun windowKey(ts: Long, delta: Int = 0): Long {
        return ((ts + windowSize * delta) / windowSize) * windowSize
    }

    private val streamer = ignite.dataStreamer<EdenPageKey, EdenPage>(FRESH_SAMPLES).apply {
        allowOverwrite(true)
        receiver(StreamTransformer.from { entry, arguments ->
            if (arguments.size != 1) {
                ignite.log().error("Invalid argument size! $arguments")
                return@from null
            }
            val sample = arguments[0] as EdenPage
            if (entry.value == null) {
                entry.value = EdenPage(sample.values, sample.timestamps)
            } else {
                val v = entry.value
                val newVals = Arrays.copyOf(v.values, v.values.size + sample.values.size)
                System.arraycopy(sample.values, 0, newVals, v.values.size, sample.values.size)
                val newTimestamps = Arrays.copyOf(v.timestamps, v.timestamps.size + sample.timestamps.size)
                System.arraycopy(sample.timestamps, 0, newTimestamps, v.timestamps.size, sample.timestamps.size)
                entry.value = EdenPage(newVals, newTimestamps) // renew instance to take effect
            }
            return@from null
        })
    }

    fun push(instance: String, ts: Long, samples: Collection<ScrapedSample>, flush: Boolean = true) {
        samples.forEach { s ->
            val metricID = Metric.labelsFingerprintFNV(s.m)
            val p = EdenPage(
                    doubleArrayOf(s.value),
                    longArrayOf(ts)
            )
            streamer.addData(EdenPageKey(instance, metricID, windowKey(ts)), p)
        }
        if (flush) {
            streamer.flush() // should be async, but put/get consistency might be required for testing
        }
    }

    fun collectRange(metric: Metric, frames: TimeFrames, range: Long, offset: Long): List<Pair<LongArray, DoubleArray>> {
        val instance = metric.m[Metric.instanceLabel] ?: ""
        val metricID = metric.fingerprint()
        val collectingKeys = mutableSetOf<EdenPageKey>()
        for (originalTs in frames) {
            val t = originalTs - offset
            var pageDelta = 0
            while (true) {
                val wk = windowKey(t, pageDelta)
                collectingKeys.add(EdenPageKey(instance, metricID, wk))
                if (wk <= 0 || t - wk >= range) {
                    break
                }
                pageDelta--
            }
        }
        val m = Long2DoubleRBTreeMap()
        cache.getEntries(collectingKeys).forEach {
            val v = it.value
            for (i in 0 until v.values.size) {
                m[v.timestamps[i]] = v.values[i]
            }
        }
        return frames.map { originalTs ->
            val rangeOlder = originalTs - offset - range
            val rangeNewer = originalTs - offset
            val subMap = m.subMap(rangeOlder, rangeNewer + 1) // toKey is exclusive, so +1 to wrap rangeNewer itself
            val values = DoubleArray(subMap.size)
            val timestamps = LongArray(subMap.size)
            var ctr = 0
            subMap.long2DoubleEntrySet().forEach { e ->
                values[ctr] = e.doubleValue
                timestamps[ctr] = e.longKey
                ctr++
            }
            timestamps to values
        }
    }

    fun collectInstant(metric: Metric, range: TimeFrames, offset: Long): Series {
        val instance = metric.m[Metric.instanceLabel] ?: ""
        val metricID = metric.fingerprint()
        val collectingKeys = mutableSetOf<EdenPageKey>()
        for (t in range) {
            collectingKeys.add(EdenPageKey(instance, metricID, windowKey(t)))
            collectingKeys.add(EdenPageKey(instance, metricID, windowKey(t, -1)))
        }
        val m = Long2DoubleRBTreeMap()
        cache.getEntries(collectingKeys).forEach {
            val v = it.value
            for (i in 0 until v.values.size) {
                m[v.timestamps[i]] = v.values[i]
            }
        }
        val timestamps = LongArrayList()
        val values = DoubleArray(range.size) {
            val originalTs = range[it]
            val t = originalTs - offset
            val subMap = m.headMap(t + 1)
            if (subMap.isEmpty()) {
                timestamps.add(originalTs)
                Mat.StaleValue
            } else {
                val lastKey = subMap.lastLongKey()
                val delta = t - lastKey
                if (delta > 5 * 60 * 1000) {
                    timestamps.add(originalTs)
                    Mat.StaleValue
                } else {
                    timestamps.add(lastKey)
                    subMap[lastKey]
                }
            }
        }
        timestamps.trim()
        return Series(metric, values, timestamps.elements())
    }

    override fun close() {
        cache.close()
        streamer.close()
    }
}
