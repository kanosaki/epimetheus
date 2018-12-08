package epimetheus.storage

import epimetheus.CacheName.Prometheus.FRESH_SAMPLES
import epimetheus.model.Mat
import epimetheus.model.Metric
import epimetheus.model.Series
import epimetheus.model.TimeFrames
import epimetheus.pkg.textparse.ScrapedSample
import it.unimi.dsi.fastutil.longs.Long2DoubleRBTreeMap
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
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

    fun push(jobInstance: String, ts: Long, samples: Collection<ScrapedSample>, flush: Boolean = true) {
        samples.forEach { s ->
            val metricID = s.met.fingerprint()
            val p = EdenPage(
                    doubleArrayOf(s.value),
                    longArrayOf(s.timestamp ?: ts)
            )
            val wk = windowKey(ts)
            val instance = s.met.get(Metric.instanceLabel) ?: jobInstance
            streamer.addData(EdenPageKey(instance, metricID, wk), p)
        }
        if (flush) {
            streamer.flush() // should be async, but put/get consistency might be required for testing
        }
    }

    private fun cutPage(keys: List<EdenPageKey>, pages: Array<EdenPage?>, older: Long, newer: Long): Pair<LongArray, DoubleArray> {
        if (pages.isEmpty()) {
            return longArrayOf() to doubleArrayOf()
        }
        val newKey = windowKey(newer)
        val oldKey = windowKey(older)
        var newIndex = keys.binarySearch { it.timestamp.compareTo(newKey) }
        var oldIndex = keys.binarySearch { it.timestamp.compareTo(oldKey) }
        if (newIndex < 0) {
            newIndex = Math.min(newIndex.inv(), pages.size - 1)
        }
        if (oldIndex < 0) {
            oldIndex = Math.min(oldIndex.inv(), pages.size - 1)
        }

        val startIndexes = IntArray(newIndex - oldIndex + 1)
        val endIndexes = IntArray(newIndex - oldIndex + 1)
        // calculate return buffer size in advance
        var retItemSize = 0
        for (pageIdx in oldIndex..newIndex) {
            val page = pages[pageIdx] ?: continue

            var copyFrom = 0
            var copyTo = page.timestamps.size - 1
            if (pageIdx == oldIndex) {
                val startIdx = Arrays.binarySearch(page.timestamps, older)
                copyFrom = if (startIdx < 0) {
                    startIdx.inv()
                } else {
                    startIdx
                }
            }
            if (pageIdx == newIndex) {
                val toIndex = Arrays.binarySearch(page.timestamps, newer)
                copyTo = if (toIndex < 0) {
                    toIndex.inv() - 1
                } else {
                    toIndex
                }
            }
            val copyLen = copyTo - copyFrom + 1
            retItemSize += copyLen
            startIndexes[pageIdx - oldIndex] = copyFrom
            endIndexes[pageIdx - oldIndex] = copyTo
        }
        val tsArray = LongArray(retItemSize)
        val vArray = DoubleArray(retItemSize)

        // fill up array
        var copiedSize = 0
        loop@ for (pageIdx in oldIndex..newIndex) {
            val page = pages[pageIdx] ?: continue
            val copyFrom = startIndexes[pageIdx - oldIndex]
            val copyTo = endIndexes[pageIdx - oldIndex]
            val copyLen = copyTo - copyFrom + 1
            System.arraycopy(page.timestamps, copyFrom, tsArray, copiedSize, copyLen)
            System.arraycopy(page.values, copyFrom, vArray, copiedSize, copyLen)
            copiedSize += copyLen
        }
        return tsArray to vArray
    }

    fun collectRange(metric: Metric, frames: TimeFrames, range: Long, offset: Long): List<Pair<LongArray, DoubleArray>> {
        assert(range > 0)
        val instance = metric.get(Metric.instanceLabel) ?: ""
        val metricID = metric.fingerprint()
        val wkCache = LongOpenHashSet(frames.size * (range / windowSize).toInt())
        // timestamp sorted keys
        val collectingKeys = mutableListOf<EdenPageKey>()
        for (originalTs in frames) {
            val t = originalTs - offset - range
            var pageDelta = 0
            while (true) {
                val wk = windowKey(t, pageDelta)
                pageDelta++
                if (wkCache.contains(wk)) {
                    continue
                }
                collectingKeys.add(EdenPageKey(instance, metricID, wk))
                wkCache.add(wk)
                if (wk > originalTs - offset) {
                    break
                }
            }
        }
        val entriesBuffer = Array<EdenPage?>(collectingKeys.size) { null }
        collectingKeys.withIndex().toList().parallelStream().forEach {
            entriesBuffer[it.index] = cache[it.value]
        }
        return frames.map { originalTs ->
            val rangeOlder = originalTs - offset - range
            val rangeNewer = originalTs - offset
            cutPage(collectingKeys, entriesBuffer, rangeOlder, rangeNewer)
        }
    }

    fun collectInstant(metric: Metric, range: TimeFrames, offset: Long): Series {
        val instance = metric.get(Metric.instanceLabel) ?: ""
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

    fun clearData() {
        cache.clear()
    }
}
