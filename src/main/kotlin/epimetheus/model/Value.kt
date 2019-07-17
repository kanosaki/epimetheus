package epimetheus.model

import epimetheus.pkg.promql.PromQLException
import tech.tablesaw.api.StringColumn
import tech.tablesaw.api.Table
import tech.tablesaw.columns.Column
import java.util.*
import kotlin.Comparator

interface RuntimeValue : Value { //temp name

}

interface RNumber : RuntimeValue {
    fun at(idx: Int): Double
}

data class RConstant(val value: Double) : RNumber {
    override fun at(idx: Int): Double {
        return value
    }
}

data class RNumberVector(val values: DoubleSlice) : RNumber {
    override fun at(idx: Int): Double {
        return values[idx]
    }
}

data class RString(val value: String) : RuntimeValue

data class RPoints(val timestamps: LongSlice, val values: DoubleSlice) : RuntimeValue {
    init {
        assert(timestamps.size == values.size)
    }

    fun duplicate(): RPoints {
        return RPoints(timestamps, DoubleSlice(values.values.clone(), values.begin, values.size))
    }

    inline fun mapValues(fn: (Double, Long, Int) -> Double): RPoints {
        val mappedArray = DoubleArray(values.size) { i ->
            fn(values[i], timestamps[i], i)
        }
        return RPoints(timestamps, DoubleSlice.wrap(mappedArray))
    }

    fun isEmpty(): Boolean {
        return values.isEmpty() || values.isAllStale()
    }

    companion object {
        inline fun init(timestamps: List<Long>, fn: (Int, Long) -> Double): RPoints {
            val ts = LongSlice.wrap(timestamps.toLongArray())
            val vals = DoubleSlice.init(ts.size) { idx ->
                fn(idx, ts[idx])
            }
            return RPoints(ts, vals)
        }
    }
}

data class RRanges(val ranges: List<RPoints>)

interface RData : RuntimeValue {
    val metrics: List<Metric>
    val offset: Long
}

data class RRangeMatrix(override val metrics: List<Metric>, val chunks: List<RRanges>, val frames: TimeFrames, val range: Long, override val offset: Long = 0) : RData {
    inline fun unify(fn: (Int, Long, LongSlice, DoubleSlice) -> Double): RPointMatrix {
        val frameSlice = LongSlice.wrap(frames.toLongArray()) // TODO: optimize
        return RPointMatrix(metrics, chunks.map { rRanges ->
            val vs = DoubleSlice.init(rRanges.ranges.size) { i ->
                val ts = frames[i]
                val ranges = rRanges.ranges[i]
//                assert(ranges.timestamps.size == 1 || ranges.timestamps.first() <= ts && ts <= ranges.timestamps.last()) {
//                    "skewed timestamp! ${ranges.timestamps.first()}(range begin) <= $ts(frame timestamp) <= ${ranges.timestamps.last()}(range end)"
//                }
                fn(i, ts, ranges.timestamps, ranges.values)
            }
            RPoints(frameSlice, vs)
        }, frames)
    }

    companion object {
        fun merge(matrixes: List<RRangeMatrix>, frames: TimeFrames, range: Long): RRangeMatrix {
            val metrics = matrixes.flatMap { it.metrics }
            val ranges = matrixes.flatMap { it.chunks }
            return RRangeMatrix(metrics, ranges, frames, range)
        }
    }
}

object MetricComparator: Comparator<Metric> {
    override fun compare(o1: Metric?, o2: Metric?): Int {
        val itFp = o1!!.fingerprint()
        val metFp = o2!!.fingerprint()
        return when {
            itFp > metFp -> 1
            itFp < metFp -> -1
            else -> 0
        }
    }
}

class RPointMatrixBuilder(var frames: TimeFrames, var offset: Long = 0, val sort: Boolean = false, val allowDuplication: Boolean = false) {
    private val metrics = LinkedList<Metric>()
    private val series = LinkedList<RPoints>()
    private var sortByValueDesc: Boolean? = null

    fun add(met: Metric, ser: RPoints): RPointMatrixBuilder {
        if (sort) {
            val insertAt = metrics.binarySearch(met, MetricComparator)
            if (insertAt >= 0) {
                if (!allowDuplication) {
                    throw PromQLException("duplicated metric: $met")
                }
            } else {
                metrics.add(insertAt.inv(), met)
                series.add(insertAt.inv(), ser)
            }
        } else {
            metrics.add(met)
            series.add(ser)
        }
        return this
    }

    fun sortByValue(desc: Boolean): RPointMatrixBuilder {
        if (sort) {
            throw RuntimeException("cannot sort by value on sorted (by fingerprint) builder")
        }
        sortByValueDesc = desc
        return this
    }

    fun build(): RPointMatrix {
        val sbvd = sortByValueDesc ?: return RPointMatrix(metrics, series, frames, offset)

        class Row(val m: Metric, val s: RPoints)

        val rows = Array(metrics.size) { i ->
            Row(metrics[i], series[i])
        }
        val comparator = Comparator<Row> { o1, o2 ->
            val sign = if (sbvd) -1 else 1
            val v1 = o1.s.values[o1.s.values.size - 1]
            val v2 = o2.s.values[o2.s.values.size - 1]
            if (!v1.isFinite()) {
                1
            } else if (!v2.isFinite()) {
                -1
            } else {
                sign * v1.compareTo(v2)
            }
        }
        rows.sortWith(comparator)
        return RPointMatrix(rows.map { it.m }, rows.map { it.s }, frames, offset)
    }
}

data class RPointMatrix(override val metrics: List<Metric>, val series: List<RPoints>, val frames: TimeFrames, override val offset: Long = 0) : RData {
    init {
        assert(metrics.size == series.size)
    }

    val rowCount: Int
        get() = metrics.size

    val colCount: Int
        get () = if (series.isEmpty()) 0 else series[0].timestamps.size

    // clone values, but not for metrics and timestamps
    fun duplicate(): RPointMatrix {
        return RPointMatrix(metrics, series.map { it.duplicate() }, frames, offset)
    }

    inline fun mapValues(fn: (Double, Long, Int) -> Double): RPointMatrix {
        return RPointMatrix(metrics, series.map { it.mapValues(fn) }, frames, offset)
    }

    inline fun mapRow(fn: (DoubleSlice, LongSlice, Int) -> DoubleSlice): RPointMatrix {
        return RPointMatrix(
                metrics,
                series.mapIndexed { index, rp ->
                    RPoints(rp.timestamps, fn(rp.values, rp.timestamps, index))
                }
                , frames, offset)
    }

    fun isIsomorphic(other: RPointMatrix): Boolean {
        if (this.metrics != other.metrics) return false
        if (this.series.size != other.series.size) return false
        for (i in 0 until this.series.size) {
            if (this.series[i].timestamps != other.series[i].timestamps) {
                return false
            }
        }
        return true
    }

    fun toShapeTable(tableName: String = ""): Table {
        val usedNames = mutableSetOf<String>()
        fun mkUniqueName(n: String): String {
            var name = n
            var ctr = 1
            while (usedNames.contains(name)) {
                name = "$n$ctr"
                ctr++
            }
            usedNames += name
            return name
        }
        if (series.isEmpty()) {
            return Table.create(tableName)
        }

        val timestamps = series.map { it.timestamps }
        val cols = Array<Column<*>>(series.first().timestamps.size + 1) { i ->
            if (i == 0) {
                StringColumn.create("shape", metrics.map { it.toString() })
            } else {
                StringColumn.create(mkUniqueName(frames[i - 1].toString()), timestamps.map { it[i - 1].toString() })
            }
        }
        return Table.create(tableName, *cols)
    }

    fun prune(): RPointMatrix {
        val mets = mutableListOf<Metric>()
        val sels = mutableListOf<RPoints>()
        for (i in 0 until metrics.size) {
            val ss = series[i]
            if (!ss.values.isEmpty() && !ss.values.isAllStale()) {
                mets.add(metrics[i])
                sels.add(series[i])
            }
        }
        return RPointMatrix(mets, sels, frames, offset)
    }

    fun sortSeries(): RPointMatrix {
        class Row(val m: Metric, val s: RPoints)

        val rows = Array(metrics.size) { i ->
            Row(metrics[i], series[i])
        }
        rows.sortBy { it.m.fingerprint() }
        return RPointMatrix(rows.map { it.m }, rows.map { it.s }, frames, offset)
    }

    fun sortSeriesByLastValue(desc: Boolean = false): RPointMatrix {
        if (this.colCount == 0) {
            return this
        }
        class Row(val m: Metric, val s: RPoints)

        val rows = Array(metrics.size) { i ->
            Row(metrics[i], series[i])
        }
        val comparator = Comparator<Row> { o1, o2 ->
            val sign = if (desc) -1 else 1
            val v1 = o1.s.values[o1.s.values.size - 1]
            val v2 = o2.s.values[o2.s.values.size - 1]
            if (!v1.isFinite()) {
                1
            } else if (!v2.isFinite()) {
                -1
            } else {
                sign * v1.compareTo(v2)
            }
        }
        rows.sortWith(comparator)
        return RPointMatrix(rows.map { it.m }, rows.map { it.s }, frames, offset)
    }

    fun toTable(tableName: String = ""): Table {
        val usedNames = mutableSetOf<String>()
        fun mkUniqueName(n: String): String {
            var name = n
            var ctr = 1
            while (usedNames.contains(name)) {
                name = "$n$ctr"
                ctr++
            }
            usedNames += name
            return name
        }
        if (series.isEmpty()) {
            return Table.create(tableName)
        }

        val timestamps = series.flatMap { it.timestamps }.toSortedSet().toList()

        val cols = Array<Column<*>>(timestamps.size + 1) { i ->
            if (i == 0) {
                StringColumn.create(mkUniqueName("metric"), metrics.map { it.toString() })
            } else {
                val ts = timestamps[i - 1]
                StringColumn.create(
                        mkUniqueName(ts.toString()),
                        series.map {
                            val valIdx = it.timestamps.indexOf(ts)
                            if (valIdx >= 0) {
                                Mat.formatValue(it.values[valIdx])
                            } else {
                                "-" // not found
                            }
                        }
                )
            }
        }
        return Table.create(tableName, *cols)
    }


    companion object {
        fun of(frames: TimeFrames, vararg series: Pair<Metric, List<Double>>, sort: Boolean = true): RPointMatrix {
            val metrics = series.map { it.first }
            val tses = LongSlice.wrap(frames.toLongArray())
            val sels = series.map { RPoints(tses, DoubleSlice.wrap(it.second.toDoubleArray())) }
            if (sort) {
                return RPointMatrix(metrics, sels, frames).sortSeries()
            } else {
                return RPointMatrix(metrics, sels, frames)
            }
        }

        fun merge(matrixes: Iterable<RPointMatrix>, frames: TimeFrames): RPointMatrix {
            val builder = RPointMatrixBuilder(frames, sort = true)
            for (rpm in matrixes) {
                for (metIdx in 0 until rpm.metrics.size) {
                    builder.add(rpm.metrics[metIdx], rpm.series[metIdx])
                }
            }
            return builder.build()
        }
    }
}

