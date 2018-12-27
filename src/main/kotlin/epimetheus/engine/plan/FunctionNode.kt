package epimetheus.engine.plan


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
