package epimetheus.storage

import epimetheus.model.*
import epimetheus.pkg.textparse.ScrapedSample
import java.util.*

class MockGateway() : Gateway, MetricRegistory {
    //  NOTE: sortedmap sorts samples by ASC order
    // (timestamp, metricID) -> ScrapedSample
    val datum = mutableMapOf<Signature, SortedMap<Long, Double>>()
    val metrics = mutableMapOf<Signature, Metric>()

    override fun pushScraped(instance: String, ts: Long, mets: Collection<ScrapedSample>) {
        mets.forEach {
            val met = Metric(it.m)
            val prev = datum.getOrDefault(met.fingerprint(), sortedMapOf())
            prev[ts] = it.value
            datum[met.fingerprint()] = prev
            metrics[met.fingerprint()] = met
        }
    }

    override fun collectSeries(query: MetricMatcher, range: TimeFrames): VarMat {
        val serieses = metrics.values
                .filter { query.matches(it) }
                .map {
                    val wholeData = datum[it.fingerprint()]!!
                    Series(it, wholeData.values.toDoubleArray(), wholeData.keys.toLongArray())
                }
                .sortedBy { it.metric.fingerprint() }
        val metrics= serieses.map {it.metric }.toTypedArray()
        return VarMat(metrics, serieses)
    }

    override fun collectGrid(query: MetricMatcher, range: TimeFrames): GridMat {
        val serieses = metrics.values
                .filter { query.matches(it) }
                .mapNotNull {
                    val sig = it.fingerprint()
                    val wholeData = datum[sig]!!
                    val sampleTs = wholeData.keys.toLongArray()
                    val sampleData = wholeData.values.toDoubleArray()
                    var seekPtr = 0

                    val values = DoubleArray(range.size)
                    for (i in 0 until values.size) {
                        val ts = range[i]
                        // to put reverse order
                        val putIdx = values.size - i - 1
                        var set = false
                        for (seek in seekPtr until sampleTs.size) {
                            seekPtr = seek
                            if (seek == sampleTs.size - 1) {
                                if (ts - sampleTs[seek] <= Gateway.StaleSearchMilliseconds) {
                                    values[putIdx] = sampleData[seek]
                                    set = true
                                }
                                break
                            }
                            if (sampleTs[seek + 1] > ts) {
                                values[putIdx] = sampleData[seek]
                                set = true
                                break
                            }
                        }
                        if (!set) {
                            values[putIdx] = Mat.StaleValue
                        }
                    }
                    if (values.all { it != Mat.StaleValue }) {
                        Series(it, values, range.toLongArray())
                    } else {
                        null
                    }
                }
                .sortedBy { it.metric.fingerprint() }
        return GridMat.concatSeries(serieses, range)
    }

    override fun metric(metricId: Long): Metric? {
        return metrics[metricId]
    }
}