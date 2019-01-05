package epimetheus.storage

import epimetheus.CacheName
import epimetheus.engine.plan.RPointMatrix
import epimetheus.engine.plan.RRangeMatrix
import epimetheus.engine.plan.RRanges
import epimetheus.model.*
import epimetheus.pkg.textparse.ExporterParser
import epimetheus.pkg.textparse.ScrapedSample
import org.antlr.v4.runtime.CharStreams
import org.apache.ignite.Ignite
import org.apache.ignite.lang.IgniteCallable
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.InputFile
import kotlin.streams.toList


interface Gateway {
    companion object {
        val StaleSearchMilliseconds = 5 * 60 * 1000
    }

    fun pushScraped(ts: Long, mets: Collection<ScrapedSample>, flush: Boolean = true)

    fun fetchInstant(metrics: List<Metric>, frames: TimeFrames, offset: Long = 0): RPointMatrix
    fun fetchRange(metrics: List<Metric>, frames: TimeFrames, range: Long, offset: Long = 0): RRangeMatrix

    fun collectInstant(query: MetricMatcher, range: TimeFrames, offset: Long = 0): GridMat
    /**
     * @param frames Collect upper points
     * @param range Collecting Range by milliseconds
     */
    fun collectRange(query: MetricMatcher, frames: TimeFrames, range: Long, offset: Long = 0L): RangeGridMat

    val metricRegistry: MetricRegistry
}

class IgniteGateway(val ignite: Ignite) : Gateway, AutoCloseable {
    val eden = EdenPageStore(ignite)
    val aged = Aged(ignite)
    override val metricRegistry = IgniteMeta(ignite)

    override fun pushScraped(ts: Long, mets: Collection<ScrapedSample>, flush: Boolean) {
        metricRegistry.registerMetricsFromSamples(mets)
        eden.push(ts, mets, flush)
    }

    override fun fetchInstant(metrics: List<Metric>, frames: TimeFrames, offset: Long): RPointMatrix {
        val vals = metrics
                .parallelStream()
                .map { eden.fetchInstant(it, frames, offset) }.toList()
        return RPointMatrix(metrics, vals, frames)
    }

    override fun fetchRange(metrics: List<Metric>, frames: TimeFrames, range: Long, offset: Long): RRangeMatrix {
        val vals = metrics
                .map { met ->
                    RRanges(eden.fetchRange(met, frames, range, offset))
                }
        return RRangeMatrix(metrics, vals.toList(), frames, range, offset)
    }

    override fun collectRange(query: MetricMatcher, frames: TimeFrames, range: Long, offset: Long): RangeGridMat {
        val mets = metricRegistry.lookupMetrics(query)
        val vals = mets.map {
            eden.collectRange(it, frames, range, offset)
        }
        return RangeGridMat(mets, frames, range, vals.toList(), offset)
    }

    // metric_name{label1=~"pat"}
    override fun collectInstant(query: MetricMatcher, range: TimeFrames, offset: Long): GridMat {
        val mets = metricRegistry.lookupMetrics(query)
        val vals = mets.parallelStream().map { eden.collectInstant(it, range, offset) }.toList()
        return GridMat.concatSeries(vals, range)
    }

    fun <V> affinityCall(metric: Metric, callable: IgniteCallable<V>): V {
        return ignite.compute().affinityCall(CacheName.Prometheus.FRESH_SAMPLES, metric.fingerprint(), callable)
    }

    override fun close() {
        eden.close()
        metricRegistry.close()
    }

    fun clearData(range: TimeFrames?) {
        if (range != null) TODO()
        eden.clearData()
    }

    fun importParquet(inputFile: InputFile) {
        val pfr = ParquetFileReader(inputFile, ParquetReadOptions.builder().build())
        val metadata = pfr.footer
        val schema = metadata.fileMetaData.schema
        val columns = schema.columns
        val mets = columns.drop(1).map {
            val metExpr = it.path.joinToString("")
            ExporterParser.parseMetric(CharStreams.fromString(metExpr))
        }
        var page = pfr.readNextRowGroup()
        var ctr = 0
        while (page != null) {
            val colIo = ColumnIOFactory().getColumnIO(schema)
            val recordReader = colIo.getRecordReader(page, GroupRecordConverter(schema))
            for (i in 0 until page.rowCount) {
                val g = recordReader.read()
                val ts = g.getLong(0, 0)
                val instanceGroup = mutableMapOf<String, MutableList<ScrapedSample>>()
                for (c in 1 until columns.size) {
                    val v = g.getDouble(c, 0)
                    val met = mets[c - 1]
                    val instance = met.get(Metric.instanceLabel) ?: ""
                    val group = instanceGroup[instance]
                    if (group != null) {
                        group.add(ScrapedSample(met, v))
                    } else {
                        instanceGroup[instance] = mutableListOf(ScrapedSample(met, v))
                    }
                    ctr++
                }
                instanceGroup.forEach { instance, samples ->
                    this.pushScraped(ts, samples, false)
                }
            }
            page = pfr.readNextRowGroup()
        }
        this.pushScraped(0, listOf(), true)
        println("Loaded $ctr samples")
    }
}