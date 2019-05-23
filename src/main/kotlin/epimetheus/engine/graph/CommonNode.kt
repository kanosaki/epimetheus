package epimetheus.engine.graph

import epimetheus.DurationUtil.toPromString
import epimetheus.engine.EngineContext
import epimetheus.engine.ExecContext
import epimetheus.engine.plan.*
import epimetheus.model.Metric
import epimetheus.pkg.promql.InstantSelector
import epimetheus.pkg.promql.PromQLException
import java.time.Duration

interface PlanNode {
    /**
     * ExecContext:: serializable and it will be send over network
     * EngineContext:: non-serializable and assigned by engine on each local node
     */
    fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue

    fun evalWithTrace(node: PlanNode, ec: ExecContext, eng: EngineContext): RuntimeValue {
        // TODO: Add tracing here
        val evalBegin = System.nanoTime()
        val res = node.evaluate(ec, eng)
        val evalEnd = System.nanoTime()
        ec.tracer.addTrace(ec, this, node, res, evalBegin, evalEnd)
        return res
    }

    val affinity: NodeAffinity

    fun reprNode(): String {
        return this.toString()
    }

    fun reprRecursive(): String {
        return this.toString()
    }
}

object RootNode : PlanNode {
    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        TODO("not implemented")
    }

    override val affinity: NodeAffinity = NodeAffinity.Any
    override fun toString(): String {
        return "RootNode"
    }
}

interface InstantNode : PlanNode {
    val metPlan: MetricPlan
}

interface FixedInstantNode : InstantNode {
    override val metPlan: FixedMetric
}

data class MatrixSelectorNode(
        val metric: Metric,
        val range: Duration,
        val offset: Duration
) : FixedInstantNode {
    override val metPlan: FixedMetric = FixedMetric(listOf(metric))
    override val affinity: NodeAffinity = NodeAffinity.Single(metric)

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        return eng.gateway.fetchRange(listOf(metric), ec.frames, range.toMillis(), offset.toMillis())
    }

    override fun reprNode(): String {
        return "$metric[${range.toPromString()}]${if (!offset.isZero) "offset ${offset.toPromString()}" else ""}"
    }
}

data class InstantSelectorNode(
        val ast: InstantSelector, // used in 'absent' function
        val metric: Metric,
        val offset: Duration
) : FixedInstantNode {
    override val metPlan: FixedMetric = FixedMetric(listOf(metric))
    override val affinity: NodeAffinity = NodeAffinity.Single(metric)

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        return eng.gateway.fetchInstant(listOf(metric), ec.frames, offset.toMillis())
    }

    override fun reprNode(): String {
        return "$metric${if (!offset.isZero) "offset ${offset.toPromString()}" else ""}"
    }
}

data class BoolConvertNode(
        override val metPlan: MetricPlan,
        val value: PlanNode
) : InstantNode {
    override val affinity: NodeAffinity = value.affinity

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val v = evalWithTrace(value, ec, eng)
        return when (v) {
            is RConstant -> RConstant(ValueUtils.boolConvert(v.value))
            is RScalarVector -> RScalarVector(v.values.mapCopy { _, d -> ValueUtils.boolConvert(d) })
            is RPointMatrix -> {
                val m = v.mapValues { d, _, _ ->
                    ValueUtils.boolConvert(d)
                }
                val mets = v.metrics.map { it.filterWithout(true, listOf()) }
                RPointMatrix(mets, m.series, v.frames, v.offset)
            }
            else -> throw PromQLException("cannot convert ${v.javaClass} to bool")
        }
    }
}

data class StringLiteralNode(val value: String) : PlanNode {
    override val affinity: NodeAffinity = NodeAffinity.Any

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        return RString(value)
    }
}

data class ScalarLiteralNode(val value: Double) : PlanNode {
    override val affinity: NodeAffinity = NodeAffinity.Any

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        return RConstant(value)
    }
}

