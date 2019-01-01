package epimetheus.engine.graph

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

    val affinity: NodeAffinity
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
}

data class BoolConvertNode(
        override val metPlan: MetricPlan,
        val value: PlanNode
) : InstantNode {
    override val affinity: NodeAffinity = value.affinity

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val v = value.evaluate(ec, eng)
        return when (v) {
            is RScalar -> RScalar(ValueUtils.boolConvert(v.value))
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
        return RScalar(value)
    }
}

