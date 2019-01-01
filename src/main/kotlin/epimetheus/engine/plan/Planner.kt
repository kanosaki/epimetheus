package epimetheus.engine.plan

import epimetheus.engine.EngineContext
import epimetheus.engine.graph.*
import epimetheus.engine.primitive.Aggregator
import epimetheus.engine.primitive.BOp
import epimetheus.engine.primitive.Function
import epimetheus.model.*
import epimetheus.pkg.promql.*
import epimetheus.storage.Gateway
import tech.tablesaw.api.StringColumn
import tech.tablesaw.api.Table
import tech.tablesaw.columns.Column

class Planner(
        val storage: Gateway,
        val binOpPlanner: BinOpPlanner = BinOpPlanner(BOp.builtinMap),
        val functionPlanner: FunctionPlanner = FunctionPlanner(Function.builtins),
        val aggregatorPlanner: AggregatorPlanner = AggregatorPlanner(Aggregator.builtins)) {

    fun plan(ast: Expression, ctx: EngineContext): PlanNode {
        return when (ast) {
            is NumberLiteral -> {
                ScalarLiteralNode(ast.value)
            }
            is StringLiteral -> {
                StringLiteralNode(ast.value)
            }
            is InstantSelector -> {
                planInstant(ast, ctx)
            }
            is MatrixSelector -> {
                planMatrix(ast, ctx)
            }
            is BinaryCall -> {
                binOpPlanner.plan(this, ast, ctx)
            }
            is AggregatorCall -> {
                aggregatorPlanner.plan(this, ast, ctx)
            }
            is FunctionCall -> {
                functionPlanner.plan(this, ast, ctx)
            }
            is BoolConvert -> {
                val v = plan(ast.expr, ctx)
                return when (v) {
                    is InstantNode -> BoolConvertNode(v.metPlan, v)
                    is ScalarLiteralNode -> ScalarLiteralNode(ValueUtils.boolConvert(v.value))
                    else -> throw PromQLException("cannot convert ${v.javaClass} to bool")
                }
            }
            else -> {
                TODO("$ast not implemented")
            }
        }
    }

    fun planInstant(ast: InstantSelector, ctx: EngineContext): MergePointNode {
        val mets = storage.metricRegistry.lookupMetrics(ast.matcher)
        return MergePointNode(mets.map { InstantSelectorNode(ast, it,  ast.offset) }, ast)
    }

    fun planMatrix(ast: MatrixSelector, ctx: EngineContext): MergeRangeNode {
        val mets = storage.metricRegistry.lookupMetrics(ast.matcher)
        return MergeRangeNode(mets.map { MatrixSelectorNode(it, ast.range, ast.offset) }, ast.range.toMillis())
    }
}

interface MetricPlan {

}

data class FixedMetric(val metrics: List<Metric>) : MetricPlan

object VariableMetric : MetricPlan

interface RuntimeValue : Value { //temp name

}

data class RScalar(val value: Double) : RuntimeValue
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
        return values.isEmpty() || values.all { Mat.isStale(it) }
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
    inline fun unify(fn: (Long, LongSlice, DoubleSlice) -> Double): RPointMatrix {
        val frameSlice = LongSlice.wrap(frames.toLongArray()) // TODO: optimize
        return RPointMatrix(metrics, chunks.map { rRanges ->
            val vs = DoubleSlice.init(rRanges.ranges.size) { i ->
                val ts = frames[i]
                val ranges = rRanges.ranges[i]
//                assert(ranges.timestamps.size == 1 || ranges.timestamps.first() <= ts && ts <= ranges.timestamps.last()) {
//                    "skewed timestamp! ${ranges.timestamps.first()}(range begin) <= $ts(frame timestamp) <= ${ranges.timestamps.last()}(range end)"
//                }
                fn(ts, ranges.timestamps, ranges.values)
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
            if (!ss.values.isEmpty() && ss.values.all { !Mat.isStale(it) }) {
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

        val values = series.map { it.values.toList() }
        val timestamps: List<Long> = if (series.isNotEmpty()) {
            val sampleTs = series[0].timestamps
            if (series.all { it.timestamps == sampleTs }) {
                sampleTs
            } else {
                TODO("implement")
            }
        } else {
            listOf()
        }

        val cols = Array<Column<*>>(timestamps.size + 1) { i ->
            if (i == 0) {
                StringColumn.create(mkUniqueName("metric"), metrics.map { it.toString() })
            } else {
                StringColumn.create(mkUniqueName(timestamps[i - 1].toString()), values.map { Mat.formatValue(it[i - 1]) })
            }
        }
        return Table.create(tableName, *cols)
    }


    companion object {
        fun of(frames: TimeFrames, vararg series: Pair<Metric, List<Double>>): RPointMatrix {
            val metrics = series.map { it.first }
            val tses = LongSlice.wrap(frames.toLongArray())
            val sels = series.map { RPoints(tses, DoubleSlice.wrap(it.second.toDoubleArray())) }
            return RPointMatrix(metrics, sels, frames).sortSeries()
        }

        fun merge(matrixes: List<RPointMatrix>, frames: TimeFrames): RPointMatrix {
            val metrics = matrixes.flatMap { it.metrics }
            val series = matrixes.flatMap { it.series }
            return RPointMatrix(metrics, series, frames)
        }
    }
}

