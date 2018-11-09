package epimetheus.model

import tech.tablesaw.api.StringColumn
import tech.tablesaw.api.Table
import tech.tablesaw.columns.Column
import java.util.*

typealias ETime = Long

class MatMatch(val mBase: Mat, val mOther: Mat, val matchIndex: IntArray) {
    companion object {
        fun oneToOne(mat1: Mat, mat2: Mat, matchOn: Boolean, labels: Collection<String>): MatMatch? {
            val metSize = mat1.metrics.size
            if (metSize != mat2.metrics.size) {
                return null
            }
            val mat2Maps = mutableMapOf<Signature, Int>()
            val mat2mets = mat2.metrics
            for (i in 0 until metSize) {
                val sig = mat2mets[i].filteredFingerprint(matchOn, labels)
                mat2Maps[sig] = i
            }
            val matchArray = IntArray(metSize) {
                val sig = mat1.metrics[it].filteredFingerprint(matchOn, labels)
                mat2Maps[sig] ?: return null
            }
            return MatMatch(mat1, mat2, matchArray)
        }
    }

    fun resultMetric(baseIndex: Int): Metric {
        if (baseIndex >= matchIndex.size) {
            throw RuntimeException("Invalid index: $baseIndex < ${matchIndex.size}")
        }
        return mBase.metrics[baseIndex].filter(listOf(), listOf(Metric.nameLabel))
    }

    fun apply(fn: (lvals: DoubleArray, rvals: DoubleArray) -> DoubleArray): Mat {
        val l = mBase
        val r = mOther
        val values = mutableListOf<DoubleArray>()
        for (mi in 0..(l.values.size - 1)) {
            val lvals = l.values[matchIndex[mi]]
            val rvals = r.values[matchIndex[mi]]
            values.add(fn(lvals, rvals))
        }
        return Mat(Array(l.metrics.size) { resultMetric(it) }, l.timestamps, values)
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

data class Mat(val metrics: Array<Metric>, val timestamps: List<Long>, val values: List<DoubleArray>) : Value {
    init {
        assert(values.size == metrics.size)
        assert(values.all { it.size == timestamps.size }) { "timestamps.size: ${timestamps.size}, values sizes: ${values.map { it.size }}" }
    }

    override fun toString(): String {
        return "<$timestamps${metrics.zip(values).asSequence().map { p -> "${p.first}: ${p.second.map { formatValue(it) }.toList()}" }.joinToString(",")}>"
    }

    fun toTable(tableName: String = ""): Table {
        val cols = Array<Column<*>>(metrics.size + 1) { i ->
            if (i == 0) {
                StringColumn.create("ts", timestamps.map { it.toString() })
            } else {
                StringColumn.create(metrics[i - 1].toString(), values[i - 1].map { formatValue(it) })
            }
        }
        return Table.create(tableName, *cols)
    }

    /**
     * Remove rows/columns all values are stale.
     */
    fun prune(): Mat {
        val retMetrics = mutableListOf<Metric>()
        val retValues = mutableListOf<DoubleArray>()
        for (i in 0 until values.size) {
            if (!values[i].all { isStale(it) }) {
                retValues.add(values[i])
                retMetrics.add(metrics[i])
            }
        }
        return Mat(retMetrics.toTypedArray(), timestamps, retValues)
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

        other as Mat

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
        val StaleValue = SpecialValue.STALE_VALUE
        private val specialValueNames = mapOf(StaleValue.toRawBits() to "STALE")

        fun isStale(v: Double): Boolean {
            return v.isNaN() && v.toRawBits() == StaleValue.toRawBits()
        }

        fun formatValue(v: Double): String {
            val bin = v.toRawBits()
            return specialValueNames[bin] ?: v.toString()
        }

        fun mapValue(vs: List<Double?>): DoubleArray {
            return DoubleArray(vs.size) {
                vs[it] ?: StaleValue
            }
        }

        /**
         * Simply joins series
         */
        fun concatSeries(series: List<Series>, frames: TimeFrames, metreg: MetricRegistory): Mat {
            val metrics = Array(series.size) { metreg.mustMetric(series[it].metricID) }
            val values = series.map { it.values }
            return Mat(metrics, frames, values)
        }

        fun of(frames: List<Long>, vararg series: Pair<Metric, DoubleArray>): Mat {
            val sortedSels = series.sortedBy { it.first.fingerprint() }
            return Mat(sortedSels.map { it.first }.toTypedArray(), frames, sortedSels.map { it.second })
        }
    }
}

data class MatRow(val mat: Mat, val index: Int) {
    val metric: Metric
        get() = mat.metrics[index]

    val timestamps: List<Long>
        get() = mat.timestamps

    val values: DoubleArray
        get() = mat.values[index]
}

data class MatCol(val mat: Mat, val index: Int) {
    val metrics: Array<Metric>
        get() = mat.metrics

    val timestamp: Long
        get() = mat.timestamps[index]

    val values: List<Double>
        get() = mat.values.map { it[index] }
}

class MatrixRowIterator(val mat: Mat) : Iterator<MatRow> {
    var index = 0
    override fun hasNext(): Boolean {
        return mat.metrics.size > index
    }

    override fun next(): MatRow {
        return MatRow(mat, index++)
    }
}

class MatrixColumnIterator(val mat: Mat) : Iterator<MatCol> {
    var index = 0
    override fun hasNext(): Boolean {
        return mat.timestamps.size > index
    }

    override fun next(): MatCol {
        return MatCol(mat, index++)
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
