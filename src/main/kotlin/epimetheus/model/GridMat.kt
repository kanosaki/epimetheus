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
        return mBase.metrics[baseIndex].filterWithout(Metric.nameLabel)
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

data class RangeGridMat(val metrics: List<Metric>, val timestamps: List<Long>, val windowSize: Long, val series: List<List<Pair<LongArray, DoubleArray>>>) : Mat {
    fun applyUnifyFn(fn: (Metric, LongArray, DoubleArray) -> Double): GridMat {
        val values = mutableListOf<DoubleArray>()
        for (mIdx in 0 until metrics.size) {
            val m = metrics[mIdx]
            val ary = DoubleArray(timestamps.size) { tIdx ->
                val pair = series[mIdx][tIdx]
                fn(m, pair.first, pair.second)
            }
            values += ary
        }
        return GridMat(metrics.toTypedArray(), timestamps, values)
    }

    override fun toString(): String {
        return "<RangeGridMat metrics=$metrics timestamps=$timestamps windowSize=$windowSize series=${series.map { s -> s.map { p -> "${p.first.toList()} ${p.second.toList()}" } }}"
    }
}

data class GridMat(val metrics: Array<Metric>, val timestamps: List<Long>, val values: List<DoubleArray>) : Mat {
    init {
        assert(values.size == metrics.size)
        assert(values.all { it.size == timestamps.size }) { "timestamps.size: ${timestamps.size}, values sizes: ${values.map { it.size }}" }
    }

    fun dropMetricName(): GridMat {
        return GridMat(metrics.map { it.filterWithout(Metric.nameLabel) }.toTypedArray(), timestamps, values)
    }

    override fun toString(): String {
        return "<$timestamps${metrics.zip(values).asSequence().map { p -> "${p.first}: ${p.second.map { Mat.formatValue(it) }.toList()}" }.joinToString(",")}>"
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

        val cols = Array<Column<*>>(metrics.size + 1) { i ->
            if (i == 0) {
                StringColumn.create(mkUniqueName("ts"), timestamps.map { it.toString() })
            } else {
                StringColumn.create(mkUniqueName(metrics[i - 1].toString()), values[i - 1].map { Mat.formatValue(it) })
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
        fun concatSeries(series: List<Series>, frames: List<Long>): GridMat {
            val metrics = Array(series.size) { series[it].metric }
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

data class Series(val metric: Metric, val values: DoubleArray, val timestamps: LongArray) : Value {
}
