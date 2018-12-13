package epimetheus.storage

import epimetheus.model.*
import epimetheus.pkg.textparse.ScrapedSample
import it.unimi.dsi.fastutil.longs.LongArrayList
import java.util.*

class MockGateway() : Gateway, MetricRegistry {
    //  NOTE: sortedmap sorts samples by ASC order
    // (timestamp, metricID) -> ScrapedSample
    val datum = mutableMapOf<Signature, SortedMap<Long, Double>>()
    val metrics = sortedMapOf<Signature, Metric>()
    override val metricRegistry = this

    override fun pushScraped(ts: Long, mets: Collection<ScrapedSample>, flush: Boolean) {
        mets.forEach {
            val met = it.met
            val prev = datum.getOrDefault(met.fingerprint(), sortedMapOf())
            prev[ts] = it.value
            datum[met.fingerprint()] = prev
            metrics[met.fingerprint()] = met
        }
    }

    override fun collectRange(query: MetricMatcher, frames: TimeFrames, range: Long, offset: Long): RangeGridMat {
        val mets = metrics.values.filter { query.matches(it) }
        val values = mets.map { m ->
            val wholeData = datum[m.fingerprint()]!!
            frames.map { f ->
                val blocks = wholeData.filterKeys { k -> (f - range - offset) <= k && k <= (f - offset) }.toList()
                val r = blocks.map { it.first }.toLongArray() to blocks.map { it.second }.toDoubleArray()
                r
            }
        }
        return RangeGridMat(mets, frames, range, values, offset)
    }

    override fun collectInstant(query: MetricMatcher, range: TimeFrames, offset: Long): GridMat {
        val serieses = metrics.values
                .filter { query.matches(it) }
                .map {
                    val sig = it.fingerprint()
                    val wholeData = datum[sig]!!
                    val timestamps = LongArrayList()
                    val v = range.map { originalTs ->
                        val ts = originalTs - offset
                        val subMap = wholeData.headMap(ts + 1)
                        if (subMap.isEmpty()) {
                            timestamps.add(originalTs)
                            Mat.StaleValue
                        } else {
                            val lk = subMap.lastKey()!!
                            val delta = ts - lk
                            if (delta > 5 * 60 * 1000) {
                                timestamps.add(originalTs)
                                Mat.StaleValue
                            } else {
                                timestamps.add(lk)
                                subMap[lk]!!
                            }
                        }
                    }
                    timestamps.trim()
                    Series(it, v.toDoubleArray(), timestamps.elements())
                }
                .sortedBy { it.metric.fingerprint() }
        return GridMat.concatSeries(serieses, range, offset)
    }

    override fun metric(metricId: Long): Metric? {
        return metrics[metricId]
    }
}