package epimetheus.storage

import epimetheus.model.GridMat
import epimetheus.model.MetricMatcher
import epimetheus.model.RangeGridMat
import epimetheus.model.TimeFrames
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.Ignite
import kotlin.streams.toList


interface Gateway {
    companion object {
        val StaleSearchMilliseconds = 5 * 60 * 1000
    }

    fun pushScraped(instance: String, ts: Long, mets: Collection<ScrapedSample>)

    fun collectInstant(query: MetricMatcher, range: TimeFrames): GridMat
    /**
     * @param frames Collect upper points
     * @param range Collecting Range by milliseconds
     */
    fun collectRange(query: MetricMatcher, frames: TimeFrames, range: Long, offset: Long): RangeGridMat
}

class IgniteGateway(private val ignite: Ignite) : Gateway {
    val fresh = Fresh(ignite)
    val aged = Aged(ignite)
    val metricRegistry = IgniteMeta(ignite)

    override fun pushScraped(instance: String, ts: Long, mets: Collection<ScrapedSample>) {
        metricRegistry.registerMetricsFromSamples(mets)
        fresh.push(instance, ts, mets)
    }

    override fun collectRange(query: MetricMatcher, frames: TimeFrames, range: Long, offset: Long): RangeGridMat {
        val mets = metricRegistry.lookupMetrics(query)
        val vals = mets.parallelStream().map { fresh.collectRange(it, frames, range, offset) }
        return RangeGridMat(mets, frames, range, vals.toList())
    }

    // metric_name{label1=~"pat"}
    override fun collectInstant(query: MetricMatcher, range: TimeFrames): GridMat {
        val mets = metricRegistry.lookupMetrics(query)
        val vals = mets.parallelStream().map { fresh.collectInstant(it, range) }
        return GridMat.concatSeries(vals.toList(), range)
    }
}