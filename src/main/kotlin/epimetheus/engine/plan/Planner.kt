package epimetheus.engine.plan

import epimetheus.model.*
import epimetheus.pkg.promql.*
import epimetheus.storage.Gateway

class Planner(
        val storage: Gateway,
        val binOpPlanner: BinOpPlanner = BinOpPlanner(BOp.builtinMap),
        val functionPlanner: FunctionPlanner = FunctionPlanner(Function.builtins),
        val aggregatorPlanner: AggregatorPlanner = AggregatorPlanner(Aggregator.builtins)) {

    fun plan(ast: Expression, ctx: PlannerContext): PlanNode {
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
                    is InstantNode -> BoolConvertNode(ast, v.metric, v)
                    is ScalarLiteralNode -> ScalarLiteralNode(ValueUtils.boolConvert(v.value))
                    else -> throw PromQLException("cannot convert ${v.javaClass} to bool")
                }
            }
            else -> {
                TODO("$ast not implemented")
            }
        }
    }

    fun planInstant(ast: InstantSelector, ctx: PlannerContext): InstantSelectorNode {
        val mets = storage.metricRegistry.lookupMetrics(ast.matcher)
        return InstantSelectorNode(ast, FixedMetric(mets), ast.offset)
    }

    fun planMatrix(ast: MatrixSelector, ctx: PlannerContext): MatrixSelectorNode {
        val mets = storage.metricRegistry.lookupMetrics(ast.matcher)
        return MatrixSelectorNode(ast, FixedMetric(mets), ast.range, ast.offset)
    }
}

class PlannerContext {
}

interface MetricPlan {

}

data class FixedMetric(val metrics: List<Metric>) : MetricPlan {

}

object VariableMetric : MetricPlan

object WildcardMetric : MetricPlan

class EvaluationContext(
        val frames: TimeFrames,
        val gateway: Gateway,
        val aggregators: Map<String, Aggregator>,
        val functions: Map<String, Function>
) {


}

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
}

data class RRanges(val ranges: List<RPoints>)

interface RData : RuntimeValue {
    val metrics: List<Metric>
    val offset: Long
}

data class RRangeMatrix(override val metrics: List<Metric>, val chunks: List<RRanges>, val range: Long, override val offset: Long = 0) : RData {
    inline fun unify(frames: List<Long>, fn: (Long, LongSlice, DoubleSlice) -> Double): RPointMatrix {
        val frameSlice = LongSlice.wrap(frames.toLongArray()) // TODO: optimize
        return RPointMatrix(metrics, chunks.map { rRanges ->
            val vs = DoubleSlice.init(rRanges.ranges.size) { i ->
                val ts = frames[i]
                val ranges = rRanges.ranges[i]
                assert(ranges.timestamps.size == 1 || ranges.timestamps.first() <= ts && ts <= ranges.timestamps.last()) {
                    "skewed timestamp! ${ranges.timestamps.first()}(range begin) <= $ts(frame timestamp) <= ${ranges.timestamps.last()}(range end)"
                }
                fn(ts, ranges.timestamps, ranges.values)
            }
            RPoints(frameSlice, vs)
        })
    }
}

data class RPointMatrix(override val metrics: List<Metric>, val series: List<RPoints>, override val offset: Long = 0) : RData {
    init {
        assert(metrics.size == series.size)
    }

    val rowCount: Int
        get() = metrics.size

    val colCount: Int
        get () = if (series.isEmpty()) 0 else series[0].timestamps.size

    // clone values, but not for metrics and timestamps
    fun duplicate(): RPointMatrix {
        return RPointMatrix(metrics, series.map { it.duplicate() })
    }

    inline fun mapValues(fn: (Double, Long, Int) -> Double): RPointMatrix {
        return RPointMatrix(metrics, series.map { it.mapValues(fn) })
    }

    inline fun mapRow(fn: (DoubleSlice, LongSlice, Int) -> DoubleSlice): RPointMatrix {
        return RPointMatrix(
                metrics,
                series.mapIndexed { index, rp ->
                    RPoints(rp.timestamps, fn(rp.values, rp.timestamps, index))
                }
        )
    }

    fun isIsomorphic(other: RPointMatrix): Boolean {
        if (this.metrics != other.metrics) return false
        if (this.series.size != other.series.size) return false
        for (i in 0..this.series.size) {
            if (this.series[i].timestamps != other.series[i].timestamps) {
                return false
            }
        }
        return true
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
        return RPointMatrix(mets, sels)
    }

    fun sortSeries(): RPointMatrix {
        class Row(val m: Metric, val s: RPoints)

        val rows = Array(metrics.size) { i ->
            Row(metrics[i], series[i])
        }
        rows.sortBy { it.m.fingerprint() }
        return RPointMatrix(rows.map { it.m }, rows.map { it.s })
    }

    companion object {
        fun of(timestamps: List<Long>, vararg series: Pair<Metric, List<Double>>): RPointMatrix {
            val metrics = series.map { it.first }
            val tses = LongSlice.wrap(timestamps.toLongArray())
            val sels = series.map { RPoints(tses, DoubleSlice.wrap(it.second.toDoubleArray())) }
            return RPointMatrix(metrics, sels).sortSeries()
        }
    }
}

