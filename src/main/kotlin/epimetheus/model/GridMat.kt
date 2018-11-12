package epimetheus.model

import tech.tablesaw.api.StringColumn
import tech.tablesaw.api.Table
import tech.tablesaw.columns.Column
import java.util.*

typealias ETime = Long

class MatMatch(val mBase: GridMat, val mOther: GridMat, val matchIndex: IntArray) {
    companion object {
        fun oneToOne(gridMat1: GridMat, gridMat2: GridMat, matchOn: Boolean, labels: Collection<String>): MatMatch? {
            val metSize = gridMat1.metrics.size
            if (metSize != gridMat2.metrics.size) {
                return null
            }
            val mat2Maps = mutableMapOf<Signature, Int>()
            val mat2mets = gridMat2.metrics
            for (i in 0 until metSize) {
                val sig = mat2mets[i].filteredFingerprint(matchOn, labels)
                mat2Maps[sig] = i
            }
            val matchArray = IntArray(metSize) {
                val sig = gridMat1.metrics[it].filteredFingerprint(matchOn, labels)
                mat2Maps[sig] ?: return null
            }
            return MatMatch(gridMat1, gridMat2, matchArray)
        }
    }

    fun resultMetric(baseIndex: Int): Metric {
        if (baseIndex >= matchIndex.size) {
            throw RuntimeException("Invalid index: $baseIndex < ${matchIndex.size}")
        }
        return mBase.metrics[baseIndex].filter(listOf(), listOf(Metric.nameLabel))
    }

    fun apply(fn: (lvals: DoubleArray, rvals: DoubleArray) -> DoubleArray): GridMat {
        val l = mBase
        val r = mOther
        val values = mutableListOf<DoubleArray>()
        for (mi in 0..(l.values.size - 1)) {
            val lvals = l.values[matchIndex[mi]]
            val rvals = r.values[matchIndex[mi]]
            values.add(fn(lvals, rvals))
        }
        return GridMat(Array(l.metrics.size) { resultMetric(it) }, l.timestamps, values)
    }
}

interface Value {

}

enum class MatJustify {
    Nearest,
    Last,
    Exact
}

data class BoolValue(val value: Boolean) : Value

data class Scalar(val value: Double) : Value

data class StringValue(val value: String) : Value

interface Mat : Value {
    companion object {
        val StaleValue = SpecialValue.STALE_VALUE
        private val specialValueNames = mapOf(StaleValue.toRawBits() to "STALE")

        fun isStale(v: Double): Boolean {
            return v.toRawBits() == StaleValue.toRawBits()
        }

        fun formatValue(v: Double): String {
            val bits = v.toRawBits()
            return specialValueNames[bits] ?: v.toString()
        }

        fun mapValue(vs: List<Double?>): DoubleArray {
            return DoubleArray(vs.size) {
                vs[it] ?: StaleValue
            }
        }

        // values should be DoubleArray?
        fun instant(metrics: Array<Metric>, timestamp: Long, values: List<Double>): GridMat {
            return GridMat(metrics, listOf(timestamp), values.map { DoubleArray(1) { i -> values[i] } })
        }
    }
}

data class WindowedMat(val windowSize: Long, val mat: Mat, val scope: TimeFrames) : Value {
    private fun arrayFold(timestamps: List<Long>, ary: DoubleArray, fn: (DoubleArray, Int, Int) -> Double): Pair<DoubleArray, LongArray> {
        assert(timestamps.size == ary.size)
        assert(ary.isNotEmpty())
        // TODO: timestamp MUST BE ASCENDING

        //  array   | --------- | --------- | -------- | -------- | -------- | -------- | -------- | -------- |
        //  scope(ts)                | ------------------------------------------------------------------- |
        //  windowSize (ts)                     | ------------------------------------------------ |
        //  to                                                                            <- - - - |
        //  from                     +    <- - - - - - |
        //      timestamps[fromIdx-1] > timestamps[toIdx] - windowSize

        var toIdx = ary.size - 1
        // heading toIdx
        while (toIdx >= 0 && !scope.includes(timestamps[toIdx])) {
            toIdx--
        }
        val initialToIdx = toIdx
        assert(toIdx > 0)
        var fromIdx = toIdx
        val valBuf = DoubleArray(toIdx + 1)
        val tsBuf = LongArray(toIdx + 1)
        while (fromIdx >= 0 && scope.includes(timestamps[fromIdx]) && toIdx >= 0 && scope.includes(timestamps[toIdx])) {
            // seek fromIdx
            while (fromIdx > 0 && timestamps[fromIdx - 1] >= timestamps[toIdx] - windowSize) {
                fromIdx--
            }
            valBuf[toIdx] = fn(ary, fromIdx, toIdx)
            tsBuf[toIdx] = timestamps[toIdx]
            toIdx-- // proceed (to backward)
        }
        return Arrays.copyOfRange(valBuf, toIdx + 1/*rollback last ++*/, initialToIdx + 1/* 3rd param of copyOfRange is exclusive*/) to
                Arrays.copyOfRange(tsBuf, toIdx + 1, initialToIdx + 1)
    }

    fun fold(fn: (DoubleArray, Int, Int) -> Double, metricMapper: (Metric) -> Metric): Mat {
        val m = mat
        return when (m) {
            is GridMat -> {
                if (m.values.isEmpty()) return GridMat(arrayOf(), listOf(), listOf()) // TODO: empty?
                val folded = m.values.map { arrayFold(m.timestamps, it, fn) }
                GridMat(m.metrics.map(metricMapper).toTypedArray(), folded[0].second.toList(), folded.map { it.first })
            }
            is VarMat -> {
                val mets = m.metrics.map(metricMapper).toTypedArray()
                VarMat(mets, m.series.mapIndexed { index, it ->
                    val folded = arrayFold(it.timestamps.toList(), it.values, fn)
                    Series(mets[index].fingerprint(), folded.first, folded.second)
                })
            }
            else -> throw RuntimeException("fold is not supported for ${m.javaClass}")
        }
    }
}

data class VarMat(val metrics: Array<Metric>, val series: List<Series>) : Mat {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as VarMat

        if (!metrics.contentEquals(other.metrics)) return false
        if (series != other.series) return false

        return true
    }

    override fun hashCode(): Int {
        var result = metrics.contentHashCode()
        result = 31 * result + series.hashCode()
        return result
    }

    fun asInstant(): GridMat? {
        if (series.isEmpty()) {
            return GridMat(arrayOf(), listOf(), listOf()) // TODO: empty object?
        }
        if (series.any { it.values.size != 1 }) {
            return null
        }
        val sampleTs = series[0].timestamps[0]
        if (series.any { it.timestamps[0] != sampleTs }) {
            return null
        }
        return Mat.instant(metrics, sampleTs, series.map { it.values[0] })
    }

    fun justify(tf: TimeFrames): GridMat {
        // temporary implement
        TODO()
    }
}

data class GridMat(val metrics: Array<Metric>, val timestamps: List<Long>, val values: List<DoubleArray>) : Mat {
    init {
        assert(values.size == metrics.size)
        assert(values.all { it.size == timestamps.size }) { "timestamps.size: ${timestamps.size}, values sizes: ${values.map { it.size }}" }
    }

    override fun toString(): String {
        return "<$timestamps${metrics.zip(values).asSequence().map { p -> "${p.first}: ${p.second.map { Mat.formatValue(it) }.toList()}" }.joinToString(",")}>"
    }

    fun toTable(tableName: String = ""): Table {
        val cols = Array<Column<*>>(metrics.size + 1) { i ->
            if (i == 0) {
                StringColumn.create("ts", timestamps.map { it.toString() })
            } else {
                StringColumn.create(metrics[i - 1].toString(), values[i - 1].map { Mat.formatValue(it) })
            }
        }
        return Table.create(tableName, *cols)
    }

    /**
     * Remove rows/columns all values are stale.
     */
    fun prune(): GridMat {
        val retMetrics = mutableListOf<Metric>()
        val retValues = mutableListOf<DoubleArray>()
        for (i in 0 until values.size) {
            if (!values[i].all { Mat.isStale(it) }) {
                retValues.add(values[i])
                retMetrics.add(metrics[i])
            }
        }
        return GridMat(retMetrics.toTypedArray(), timestamps, retValues)
    }

    fun rows(): MatrixRowIterator {
        return MatrixRowIterator(this)
    }

    fun cols(): MatrixColumnIterator {
        return MatrixColumnIterator(this)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as GridMat

        if (!Arrays.equals(metrics, other.metrics)) return false
        if (timestamps != other.timestamps) return false
        for (i in 0 until values.size) {
            if (!Arrays.equals(values[i], other.values[i])) {
                return false
            }
        }

        return true
    }

    override fun hashCode(): Int {
        var result = Arrays.hashCode(metrics)
        result = 31 * result + timestamps.hashCode()
        for (vals in values) {
            result = 31 * result + Arrays.hashCode(vals)
        }
        return result
    }

    companion object {
        /**
         * Simply joins series
         */
        fun concatSeries(series: List<Series>, frames: List<Long>, metreg: MetricRegistory): GridMat {
            val metrics = Array(series.size) { metreg.mustMetric(series[it].metricID) }
            val values = series.map { it.values }
            return GridMat(metrics, frames, values)
        }

        fun of(frames: List<Long>, vararg series: Pair<Metric, DoubleArray>): GridMat {
            val sortedSels = series.sortedBy { it.first.fingerprint() }
            return GridMat(sortedSels.map { it.first }.toTypedArray(), frames, sortedSels.map { it.second })
        }
    }
}

data class MatRow(val gridMat: GridMat, val index: Int) {
    val metric: Metric
        get() = gridMat.metrics[index]

    val timestamps: List<Long>
        get() = gridMat.timestamps

    val values: DoubleArray
        get() = gridMat.values[index]
}

data class MatCol(val gridMat: GridMat, val index: Int) {
    val metrics: Array<Metric>
        get() = gridMat.metrics

    val timestamp: Long
        get() = gridMat.timestamps[index]

    val values: DoubleArray
        get() = DoubleArray(gridMat.values.size) { gridMat.values[it][index] }

}

class MatrixRowIterator(val gridMat: GridMat) : Iterator<MatRow> {
    var index = 0
    override fun hasNext(): Boolean {
        return gridMat.metrics.size > index
    }

    override fun next(): MatRow {
        return MatRow(gridMat, index++)
    }
}

class MatrixColumnIterator(val gridMat: GridMat) : Iterator<MatCol> {
    var index = 0
    override fun hasNext(): Boolean {
        return gridMat.timestamps.size > index
    }

    override fun next(): MatCol {
        return MatCol(gridMat, index++)
    }
}

data class Series(val metricID: Long, val values: DoubleArray, val timestamps: LongArray) : Value {
    fun compareValues(other: Series): Boolean {
        if (this === other) return true

        if (!Arrays.equals(values, other.values)) return false
        if (!Arrays.equals(timestamps, other.timestamps)) return false

        return true
    }

    fun iter(): SeriesIterator {
        return SeriesIterator(this)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Series

        if (metricID != other.metricID) return false
        if (!Arrays.equals(values, other.values)) return false
        if (!Arrays.equals(timestamps, other.timestamps)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = metricID.hashCode()
        result = 31 * result + Arrays.hashCode(values)
        result = 31 * result + Arrays.hashCode(timestamps)
        return result
    }

    class SeriesIterator(val ser: Series) {
        private var index = 0

        fun next(): Boolean {
            index++
            return index < ser.values.size
        }

        fun value(): Double {
            return ser.values[index]
        }

        fun timestamp(): ETime {
            return ser.timestamps[index]
        }
    }
}
