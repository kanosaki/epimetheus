package epimetheus.engine.plan

import epimetheus.pkg.promql.PromQLException
import epimetheus.pkg.promql.VectorMatching

data class BinOpArithMatMatNode(override val metric: FixedMetric, val opMapping: List<IntArray>, val rhs: InstantNode, val lhs: InstantNode, val op: NumericBinOp) : InstantNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val lv = lhs.evaluate(ec)
        val rv = rhs.evaluate(ec)
        return when {
            lv is RPointMatrix && rv is RPointMatrix -> {
                assert(lv.isIsomorphic(rv))

                lv.mapRow { vs, ts, i ->
                    val rhsSeries = rv.series[i]
                    vs.mapCopy { j, l ->
                        op.fn(l, rhsSeries.values[j])
                    }
                }
            }
            else -> throw PromQLException("$op is not defined between $lhs and $rhs")
        }
    }
}

// Used when
// * op is Set operators
// * op is ArithAndLogic but either lhs or rhs are VariableMetric
/**
 * A binary operator node which one of operand metrics is non deterministic.
 */
data class BinOpDynamicNode(override val metric: MetricPlan, val lhs: InstantNode, val rhs: InstantNode, val op: BOp, val matching: VectorMatching) : InstantNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val lv = lhs.evaluate(ec)
        val rv = rhs.evaluate(ec)
        if (lv !is RPointMatrix || rv !is RPointMatrix) {
            throw PromQLException("$op is not defined between $lhs and $rhs")
        }
        return when (op) {
            is NumericBinOp -> {
                //assert(lv.timestamps == rv.timestamps)

                val matching = BinOpPlanner.computeMatching(matching, lv.metrics, rv.metrics, op.shouldDropMetricName)
                val resultMetrics = matching.first
                val operandMap = matching.second
                val series = operandMap.map { lrIdx ->
                    val lhsIndex = lrIdx[0]
                    val rhsIndex = lrIdx[1]
                    val lPoints = lv.series[lhsIndex]
                    val rPoints = rv.series[rhsIndex]
                    lPoints.mapValues { l, _, i ->
                        op.fn(l, rPoints.values[i])
                    }
                }
                RPointMatrix(resultMetrics, series)
            }
            is SetBinOp -> {
                val adoptIndexes = op.fn(lv.metrics, rv.metrics, matching)
                val lhsAdoptIndexes = adoptIndexes.first
                val rhsAdoptIndexes = adoptIndexes.second
                // TODO: resort metrics?
                val metrics = lhsAdoptIndexes.map { lv.metrics[it] } + rhsAdoptIndexes.map { rv.metrics[it] }
                val values = lhsAdoptIndexes.map { lv.series[it] } + rhsAdoptIndexes.map { rv.series[it] }
                RPointMatrix(metrics, values)
            }
            else -> TODO("never here")
        }
    }
}

data class BinOpArithScalarMatNode(override val metric: MetricPlan, val lhs: ScalarLiteralNode, val rhs: InstantNode, val op: NumericBinOp) : InstantNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val lv = lhs.value
        val rv = rhs.evaluate(ec)
        return when (rv) {
            // inplace version sample
            //is RPointMatrix -> {
            //    rv.values.forEach {
            //        it.mapInplace { _, v ->
            //            op.fn(lv, v)
            //        }
            //    }
            //    rv
            //}
            is RPointMatrix -> {
                val metrics = when (metric) {
                    is FixedMetric -> metric.metrics
                    is VariableMetric ->
                        if (op.shouldDropMetricName)
                            rv.metrics.map { it.filterWithout(true, listOf()) }
                        else
                            rv.metrics
                    else -> TODO("never here")
                }
                rv.mapValues { v, _, _ ->
                    op.fn(lv, v)
                }.copy(metrics = metrics)
            }
            else -> throw PromQLException("$op is not defined between $lhs and $rhs")
        }
    }
}

data class BinOpArithMatScalarNode(override val metric: MetricPlan, val lhs: InstantNode, val rhs: ScalarLiteralNode, val op: NumericBinOp) : InstantNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val rv = rhs.value
        val lv = lhs.evaluate(ec)
        return when (lv) {
            is RPointMatrix -> {
                val metrics = when (metric) {
                    is FixedMetric -> metric.metrics
                    is VariableMetric ->
                        if (op.shouldDropMetricName)
                            lv.metrics.map { it.filterWithout(true, listOf()) }
                        else
                            lv.metrics
                    else -> TODO("never here")
                }
                lv.mapValues { v, _, _ ->
                    op.fn(v, rv)
                }.copy(metrics = metrics)
            }
            else -> throw PromQLException("$op is not defined between $lhs and $rhs")
        }
    }
}

