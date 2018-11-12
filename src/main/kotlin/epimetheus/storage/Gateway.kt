package epimetheus.storage

import epimetheus.model.*
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.Ignite


interface Gateway {
    companion object {
        val StaleSearchMilliseconds = 5 * 60 * 1000
    }
    fun pushScraped(instance: String, ts: Long, mets: Collection<ScrapedSample>)
    fun collectGrid(query: MetricMatcher, range: TimeFrames): GridMat
    fun collectSeries(query: MetricMatcher, range: TimeFrames): VarMat
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

    override fun collectSeries(query: MetricMatcher, range: TimeFrames): VarMat {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    // metric_name{label1=~"pat"}
    override fun collectGrid(query: MetricMatcher, range: TimeFrames): GridMat {
        val mets = metricRegistry.lookupMetrics(query)
        return GridMat.concatSeries(mets.map { fresh.collect(it, range) }, range, metricRegistry) // TODO: parallelize
    }
}