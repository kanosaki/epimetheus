package epimetheus.storage

import epimetheus.model.Mat
import epimetheus.model.MetricMatcher
import epimetheus.model.MetricRegistory
import epimetheus.model.TimeFrames
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.Ignite


interface Gateway {
    companion object {
        val StaleSearchMilliseconds = 5 * 60 * 1000
    }
    fun pushScraped(instance: String, ts: Long, mets: Collection<ScrapedSample>)
    fun collect(query: MetricMatcher, range: TimeFrames): Mat
    val metricRegistry: MetricRegistory
}

class IgniteGateway(private val ignite: Ignite) : Gateway {
    val fresh = Fresh(ignite)
    val aged = Aged(ignite)
    override val metricRegistry = IgniteMeta(ignite)

    override fun pushScraped(instance: String, ts: Long, mets: Collection<ScrapedSample>) {
        metricRegistry.registerMetricsFromSamples(mets)
        fresh.push(instance, ts, mets)
    }

    // metric_name{label1=~"pat"}
    override fun collect(query: MetricMatcher, range: TimeFrames): Mat {
        val mets = metricRegistry.lookupMetrics(query)
        return Mat.concatSeries(mets.map { fresh.collect(it, range) }, range, metricRegistry) // TODO: parallelize
    }
}