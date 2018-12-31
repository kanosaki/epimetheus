package epimetheus.engine.plan

import epimetheus.model.Metric
import epimetheus.pkg.promql.PromQLException


class FixedFunctionNode(val fnName: String, override val metric: FixedMetric, val params: List<PlanNode>) : InstantNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val args = params.map { it.evaluate(ec) }
        val fn = ec.functions[fnName]
                ?: TODO("inconsistent state: function not found in evaluation (must be resolved in planning phase) $fnName")
        return fn.eval(ec, metric, args, params)
    }
}

class FunctionNode(val fnName: String, override val metric: MetricPlan, val params: List<PlanNode>) : InstantNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val args = params.map { it.evaluate(ec) }
        val fn = ec.functions[fnName]
                ?: TODO("inconsistent state: function not found in evaluation (must be resolved in planning phase) $fnName")

        return fn.eval(ec, metric, args, params)
    }
}


class FunctionNodeVector(val param: PlanNode) : InstantNode {
    override val metric: MetricPlan = FixedMetric(listOf(Metric.empty))

    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val arg = param.evaluate(ec) as? RScalar
                ?: throw PromQLException("argument of vector function must be scalar value")
        // TODO: can be GridMat
        return RPointMatrix(listOf(Metric.empty), listOf(RPoints.init(ec.frames) { _, _ -> arg.value }), ec.frames)
    }
}
