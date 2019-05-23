package epimetheus.engine.graph

import epimetheus.engine.EngineContext
import epimetheus.engine.ExecContext
import epimetheus.engine.plan.*
import epimetheus.engine.primitive.BOp
import epimetheus.engine.primitive.NumericBinOp
import epimetheus.engine.primitive.SetBinOp
import epimetheus.pkg.promql.PromQLException
import epimetheus.pkg.promql.VectorMatching

data class BinOpArithMatMatNode(override val metPlan: FixedMetric, val opMapping: List<IntArray>, val lhs: InstantNode, val rhs: InstantNode, val op: NumericBinOp) : FixedInstantNode {
    override val affinity: NodeAffinity = NodeAffinity.Splitted

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val lv = evalWithTrace(lhs, ec, eng)
        val rv = evalWithTrace(rhs, ec, eng)
        return when {
            lv is RPointMatrix && rv is RPointMatrix -> {
                val series = opMapping.map { lrIdx ->
                    val lhsIndex = lrIdx[0]
                    val rhsIndex = lrIdx[1]
                    if (lv.series.size <= lhsIndex || rv.series.size <= rhsIndex) {
                        throw RuntimeException("inconsistent state: mismatch between plan and data")
                    }
                    val lPoints = lv.series[lhsIndex]
                    val rPoints = rv.series[rhsIndex]
                    lPoints.mapValues { l, _, i ->
                        op.fn(l, rPoints.values[i])
                    }
                }
                RPointMatrix(metPlan.metrics, series, ec.frames)
            }
            else -> throw PromQLException("$op is not defined between $lhs and $rhs")
        }
    }

    override fun reprNode(): String {
        return "BinOp(${op.name}, mat-mat, fixed)"
    }
}

// Used when
// * op is Set operators
// * op is ArithAndLogic but either lhs or rhs are VariableMetric
/**
 * A binary operator node which one of operand metrics is non deterministic.
 */
data class BinOpDynamicNode(override val metPlan: MetricPlan, val lhs: InstantNode, val rhs: InstantNode, val op: BOp, val matching: VectorMatching) : InstantNode {
    override val affinity: NodeAffinity = NodeAffinity.Splitted

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val lv = evalWithTrace(lhs, ec, eng)
        val rv = evalWithTrace(rhs, ec, eng)
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
                RPointMatrix(resultMetrics, series, ec.frames)
            }
            is SetBinOp -> {
                val adoptIndexes = op.fn(lv.metrics, rv.metrics, matching)
                val lhsAdoptIndexes = adoptIndexes.first
                val rhsAdoptIndexes = adoptIndexes.second
                // TODO: resort metrics?
                val metrics = lhsAdoptIndexes.map { lv.metrics[it] } + rhsAdoptIndexes.map { rv.metrics[it] }
                val values = lhsAdoptIndexes.map { lv.series[it] } + rhsAdoptIndexes.map { rv.series[it] }
                RPointMatrix(metrics, values, ec.frames)
            }
            else -> TODO("never here")
        }
    }

    override fun reprNode(): String {
        return "BinOp(${op.name}, mat-mat, dynamic($matching))"
    }
}

data class BinOpArithScalarMatNode(override val metPlan: MetricPlan, val lhs: ScalarLiteralNode, val rhs: InstantNode, val op: NumericBinOp) : InstantNode {
    override val affinity: NodeAffinity = NodeAffinity.Splitted

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val lv = lhs.value
        val rv = evalWithTrace(rhs, ec, eng)
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
                val metrics = when (metPlan) {
                    is FixedMetric -> metPlan.metrics
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

    override fun reprNode(): String {
        val metstatus = if (metPlan is FixedMetric) "fixed" else "dynamic"
        return "BinOp(${op.name}, scalar-mat, $metstatus, lhs=${lhs.value})"
    }
}

data class BinOpArithMatScalarNode(override val metPlan: MetricPlan, val lhs: InstantNode, val rhs: ScalarLiteralNode, val op: NumericBinOp) : InstantNode {
    override val affinity: NodeAffinity = NodeAffinity.Splitted

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val rv = rhs.value
        val lv = evalWithTrace(lhs, ec, eng)
        return when (lv) {
            is RPointMatrix -> {
                val metrics = when (metPlan) {
                    is FixedMetric -> metPlan.metrics
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

    override fun reprNode(): String {
        val metstatus = if (metPlan is FixedMetric) "fixed" else "dynamic"
        return "BinOp(${op.name}, mat-scalar, $metstatus, rhs=${rhs.value})"
    }
}

