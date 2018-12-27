package epimetheus.engine.plan

import epimetheus.model.DoubleSlice
import epimetheus.model.LongSlice
import epimetheus.pkg.promql.BoolConvert
import epimetheus.pkg.promql.InstantSelector
import epimetheus.pkg.promql.MatrixSelector
import epimetheus.pkg.promql.PromQLException
import java.time.Duration

interface PlanNode {
    fun evaluate(ec: EvaluationContext): RuntimeValue
}

interface InstantNode : PlanNode {
    val metric: MetricPlan
}

data class MatrixSelectorNode(
        val ast: MatrixSelector,
        override val metric: FixedMetric,
        val range: Duration,
        val offset: Duration
) : InstantNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        return ec.gateway.fetchRange(metric.metrics, ec.frames, range.toMillis(), offset.toMillis())
    }
}

data class InstantSelectorNode(
        val ast: InstantSelector,
        override val metric: FixedMetric,
        val offset: Duration
) : InstantNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        return ec.gateway.fetchInstant(metric.metrics, ec.frames, offset.toMillis())
    }
}

data class BoolConvertNode(
        val ast: BoolConvert,
        override val metric: MetricPlan,
        val value: PlanNode
) : InstantNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val v = value.evaluate(ec)
        return when (v) {
            is RScalar -> RScalar(ValueUtils.boolConvert(v.value))
            is RPointMatrix -> v.mapValues { d, _, _ ->
                ValueUtils.boolConvert(d)
            }
            else -> throw PromQLException("cannot convert ${v.javaClass} to bool")
        }
    }
}

data class ContantRPointMatrixNode(override val metric: FixedMetric, val values: DoubleSlice) : InstantNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val metSize = metric.metrics.size
        val timestamps = LongSlice.wrap(ec.frames.toLongArray())
        val series = (0 until metSize).map { RPoints(timestamps, values) }
        return RPointMatrix(metric.metrics, series)
    }
}

data class StringLiteralNode(val value: String) : PlanNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        return RString(value)
    }
}

data class ScalarLiteralNode(val value: Double) : PlanNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        return RScalar(value)
    }
}

