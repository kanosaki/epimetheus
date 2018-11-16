package epimetheus.storage

import epimetheus.model.*
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.Ignite


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
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    // metric_name{label1=~"pat"}
    override fun collectInstant(query: MetricMatcher, range: TimeFrames): GridMat {
        val mets = metricRegistry.lookupMetrics(query)
        return GridMat.concatSeries(mets.map { fresh.collect(it, range) }, range) // TODO: parallelize
    }
}