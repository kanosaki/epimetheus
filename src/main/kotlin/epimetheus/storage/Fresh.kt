package epimetheus.storage

import epimetheus.CacheName.Prometheus.FRESH_SAMPLES
import epimetheus.model.Mat
import epimetheus.model.Metric
import epimetheus.model.Series
import epimetheus.model.TimeFrames
import epimetheus.pkg.textparse.ScrapedSample
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import it.unimi.dsi.fastutil.longs.Long2DoubleRBTreeMap
import it.unimi.dsi.fastutil.longs.LongArrayList
import org.apache.ignite.Ignite
import org.apache.ignite.cache.affinity.AffinityKeyMapped
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.stream.StreamTransformer


data class FreshSampleKey(
        @AffinityKeyMapped @QuerySqlField(notNull = true) val instance: String,
        @QuerySqlField(notNull = true) val metricID: Long,
        @QuerySqlField(notNull = true) val timestamp: Long)

data class FreshSample(val values: DoubleArrayList, val timestamps: LongArrayList) {
}

/**
 * FreshStorage is write intensive area. Affinity is defined by "instance" (in prometheus term)
 * to achieve better scalability (though it assumes even scrape_interval distribution.)
 * And here, capacity efficiency is compromised for write performance.
 */
class Fresh(val ignite: Ignite, val windowSize: Long = 5 * 60 * 1000) {
    private val freshCache = ignite.getOrCreateCache(CacheConfiguration<FreshSampleKey, FreshSample>().apply {
        name = FRESH_SAMPLES
        backups = 1
        setIndexedTypes(FreshSampleKey::class.java, FreshSample::class.java)
    })

    fun windowKey(ts: Long, delta: Int = 0): Long {
        return ((ts + windowSize * delta) / windowSize) * windowSize
    }

    private val streamer = ignite.dataStreamer<FreshSampleKey, FreshSample>(FRESH_SAMPLES).apply {
        allowOverwrite(true)
        receiver(StreamTransformer.from { entry, arguments ->
            if (arguments.size != 1) {
                ignite.log().error("Invalid argument size! $arguments")
                return@from null
            }
            val sample = arguments[0] as FreshSample
            if (entry.value == null) {
                entry.value = FreshSample(sample.values, sample.timestamps)
            } else {
                val v = entry.value
                v.values.addElements(v.values.size, sample.values.elements())
                v.timestamps.addElements(v.timestamps.size, sample.timestamps.elements())
                entry.value = FreshSample(v.values, v.timestamps) // renew instance to take effect
            }
            return@from null
        })
    }

    fun push(instance: String, ts: Long, samples: Collection<ScrapedSample>) {
        samples.forEach { s ->
            val metricID = Metric.labelsFingerprintFNV(s.m)
            streamer.addData(
                    FreshSampleKey(instance, metricID, windowKey(ts)),
                    FreshSample(
                            DoubleArrayList(doubleArrayOf(s.value)),
                            LongArrayList(longArrayOf(ts))
                    )
            )
        }
        streamer.flush() // should be async, but put/get consistency might be required for testing
    }

    fun collectRange(metric: Metric, frames: TimeFrames, range: Long, offset: Long): List<Pair<LongArray, DoubleArray>> {
        val instance = metric.m[Metric.instanceLabel] ?: ""
        val metricID = metric.fingerprint()
        val collectingKeys = mutableSetOf<FreshSampleKey>()
        for (originalTs in frames) {
            val t = originalTs - offset
            var pageDelta = 0
            while (true) {
                val wk = windowKey(t, pageDelta)
                collectingKeys.add(FreshSampleKey(instance, metricID, wk))
                if (wk <= 0 || t - wk < range) {
                    break
                }
                pageDelta--
            }
        }
        val m = Long2DoubleRBTreeMap()
        freshCache.getEntries(collectingKeys).forEach {
            val v = it.value
            for (i in 0 until v.values.size) {
                m[v.timestamps.getLong(i)] = v.values.getDouble(i)
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

    fun collectInstant(metric: Metric, range: TimeFrames): Series {
        val instance = metric.m[Metric.instanceLabel] ?: ""
        val metricID = metric.fingerprint()
        val collectingKeys = mutableSetOf<FreshSampleKey>()
        for (t in range) {
            collectingKeys.add(FreshSampleKey(instance, metricID, windowKey(t)))
            collectingKeys.add(FreshSampleKey(instance, metricID, windowKey(t, -1)))
        }
        val m = Long2DoubleRBTreeMap()
        freshCache.getEntries(collectingKeys).forEach {
            val v = it.value
            for (i in 0 until v.values.size) {
                m[v.timestamps.getLong(i)] = v.values.getDouble(i)
            }
        }
        val values = DoubleArray(range.size) {
            val t = range[it]
            val subMap = m.headMap(t + 1)
            if (subMap.isEmpty()) {
                Mat.StaleValue
            } else {
                val lastKey = subMap.lastLongKey()
                if (t - lastKey > windowSize) {
                    Mat.StaleValue
                } else {
                    subMap.get(lastKey)
                }
            }
        }
        return Series(metric, values, range.toLongArray())
    }

    fun close() {
        freshCache.close()
        streamer.close()
    }
}
