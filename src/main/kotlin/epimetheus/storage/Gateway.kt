package epimetheus.storage

import epimetheus.model.*
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.Ignite
import kotlin.streams.toList


interface Gateway {
    companion object {
        val StaleSearchMilliseconds = 5 * 60 * 1000
    }

    fun pushScraped(instance: String, ts: Long, mets: Collection<ScrapedSample>)

    fun collectInstant(query: MetricMatcher, range: TimeFrames, offset: Long = 0): GridMat
    /**
     * @param frames Collect upper points
     * @param range Collecting Range by milliseconds
     */
    fun collectRange(query: MetricMatcher, frames: TimeFrames, range: Long, offset: Long = 0L): RangeGridMat

    val metricRegistry: MetricRegistry
}

class IgniteGateway(private val ignite: Ignite) : Gateway, AutoCloseable {
    val eden = EdenPageStore(ignite)
    val aged = Aged(ignite)
    override val metricRegistry = IgniteMeta(ignite)

    override fun pushScraped(instance: String, ts: Long, mets: Collection<ScrapedSample>) {
        metricRegistry.registerMetricsFromSamples(mets)
        eden.push(instance, ts, mets)
    }

    override fun collectRange(query: MetricMatcher, frames: TimeFrames, range: Long, offset: Long): RangeGridMat {
        val mets = metricRegistry.lookupMetrics(query)
        val vals = mets.parallelStream().map { eden.collectRange(it, frames, range, offset) }
        return RangeGridMat(mets, frames, range, vals.toList())
    }

    // metric_name{label1=~"pat"}
    override fun collectInstant(query: MetricMatcher, range: TimeFrames, offset: Long): GridMat {
        val mets = metricRegistry.lookupMetrics(query)
        val vals = mets.parallelStream().map { eden.collectInstant(it, range, offset) }
        return GridMat.concatSeries(vals.toList(), range)
    }

    override fun close() {
        eden.close()
        metricRegistry.close()
    }
}