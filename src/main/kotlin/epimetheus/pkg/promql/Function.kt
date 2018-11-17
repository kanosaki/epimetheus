package epimetheus.pkg.promql

import epimetheus.engine.EvalNode
import epimetheus.model.GridMat
import epimetheus.model.Mat
import epimetheus.model.RangeGridMat
import epimetheus.model.Value


data class Function(
        val name: String,
        override val argTypes: List<ValueType> = listOf(ValueType.Vector),
        override val returnType: ValueType = ValueType.Vector,
        override val varidaric: Int = 0,
        val body: (List<Value>, EvalNode) -> Value) : Applicative {

    fun call(params: List<Value>, node: EvalNode): Value {
        val ret = this.body(params, node)
        return when (ret) {
            is GridMat -> ret.dropMetricName()
            else -> ret
        }
    }

    // TODO: UDF(User Defined Function) support?
    companion object {
        val builtins = listOf(
                //Function("abs") { TODO() },
                //Function("absent") { _, _ -> TODO() },
                //Function("avg_over_time", listOf(ValueType.Matrix), noGrid = true) { _, _ -> TODO() },
                //Function("ceil") { _, _ -> TODO() },
                //Function("changes", listOf(ValueType.Matrix)) { _, _ -> TODO() },
                //Function("clamp_max", listOf(ValueType.Vector, ValueType.Scalar)) { _ -> TODO() },
                //Function("clamp_min", listOf(ValueType.Vector, ValueType.Scalar)) { _ -> TODO() },
                Function("count_over_time", listOf(ValueType.Matrix)) { args, node ->
                    val m = args[0]
                    when (m) {
                        is RangeGridMat -> {
                            m.applyUnifyFn { m, ts, vs ->
                                var ctr = 0
                                for (v in vs) {
                                    if (!Mat.isStale(v)) {
                                        ctr++
                                    }
                                }
                                ctr.toDouble()
                            }
                        }
                        else -> {
                            throw PromQLException("count_over_time is not defined for type ${m.javaClass}")
                        }
                    }
                },
                //Function("days_in_month", varidaric = 1) { _ -> TODO() },
                //Function("days_of_month", varidaric = 1) { _ -> TODO() },
                //Function("days_of_week", varidaric = 1) { _ -> TODO() },
                //Function("day_of_month", varidaric = 1) { _ -> TODO() }, // TODO: check
                //Function("day_of_week", varidaric = 1) { _ -> TODO() }, // TODO: check
                //Function("delta", listOf(ValueType.Matrix)) { _ -> TODO() },
                //Function("deriv", listOf(ValueType.Matrix)) { _ -> TODO() },
                //Function("exp") { _ -> TODO() },
                //Function("floor") { _ -> TODO() },
                //Function("histogram_quantile", listOf(ValueType.Scalar, ValueType.Vector)) { _ -> TODO() },
                //Function("holt_winters", listOf(ValueType.Matrix, ValueType.Scalar, ValueType.Scalar)) { _ -> TODO() },
                //Function("hour", varidaric = 1) { _ -> TODO() },
                //Function("idelta", listOf(ValueType.Matrix)) { _ -> TODO() },
                //Function("increase", listOf(ValueType.Matrix)) { _ -> TODO() },
                //Function("irate", listOf(ValueType.Matrix)) { _ -> TODO() },
                //Function("label_replace", listOf(ValueType.Vector, ValueType.String, ValueType.String, ValueType.String, ValueType.String)) { _ -> TODO() },
                //Function("label_join", listOf(ValueType.Vector, ValueType.String, ValueType.String, ValueType.String), varidaric = -1) { _ -> TODO() },
                //Function("ln") { _ -> TODO() },
                //Function("log10") { _ -> TODO() },
                //Function("log2") { _ -> TODO() },
                //Function("max_over_time", listOf(ValueType.Matrix), noGrid = true) { _ -> TODO() },
                //Function("min_over_time", listOf(ValueType.Matrix), noGrid = true) { _ -> TODO() },
                //Function("minute", varidaric = 1) { _ -> TODO() },
                //Function("month", varidaric = 1) { _ -> TODO() },
                //Function("predict_linear", listOf(ValueType.Matrix, ValueType.Scalar)) { _ -> TODO() },
                //Functin("quantile_over_time", listOf(ValueType.Scalar, ValueType.Matrix), noGrid = true) { _ -> TODO() },
                Function("rate", listOf(ValueType.Matrix)) { args, node ->
                    // TODO: implement correctly
                    val m = args[0]
                    when (m) {
                        is RangeGridMat -> {
                            m.applyUnifyFn { met, ts, vs ->
                                when {
                                    vs.size < 2 -> Mat.StaleValue
                                    //else -> (vs.last() - vs.first()) / ((ts.last() - ts.first()) / 1000) // ms -> s
                                    else -> {
                                        var leapAdjust = 0.0
                                        for (i in 1 until vs.size) {
                                            if (vs[i] < vs[i - 1]) {
                                                leapAdjust += vs[i-1] - vs[i]
                                            }
                                        }
                                        ((vs.last() + leapAdjust) - vs.first()) / (m.windowSize / 1000) // ms -> s
                                    }
                                }
                            }
                        }
                        else -> {
                            throw PromQLException("rate is not defined for type ${m.javaClass}")
                        }
                    }
                }
                //Function("resets", listOf(ValueType.Matrix)) { _ -> TODO() },
                //Function("round", listOf(ValueType.Vector, ValueType.Scalar), varidaric = 1) { _ -> TODO() },
                //Function("scalar", returnType = ValueType.Scalar, varidaric = 1) { _ -> TODO() },
                //Function("sort") { _ -> TODO() },
                //Function("sort_desc") { _ -> TODO() },
                //Function("sqrt") { _ -> TODO() },
                //Function("stddev_over_time", listOf(ValueType.Matrix), noGrid = true) { _ -> TODO() },
                //Function("stdvar_over_time", listOf(ValueType.Matrix), noGrid = true) { _ -> TODO() },
                //Function("sum_over_time", listOf(ValueType.Matrix), noGrid = true) { _ -> TODO() },
                //Function("time", listOf(), returnType = ValueType.Scalar) { _ -> TODO() },
                //Function("timestamp") { _ -> TODO() },
                //Function("vector", listOf(ValueType.Scalar)) { _ -> TODO() },
                //Function("year", varidaric = 1) { _ -> TODO() }
        )
    }
}

