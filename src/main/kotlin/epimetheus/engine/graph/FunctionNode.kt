package epimetheus.engine.graph

import epimetheus.engine.EngineContext
import epimetheus.engine.ExecContext
import epimetheus.engine.plan.FixedMetric
import epimetheus.engine.plan.MetricPlan
import epimetheus.engine.plan.RuntimeValue


data class FixedFunctionNode(val fnName: String, override val metPlan: FixedMetric, val params: List<PlanNode>, override val affinity: NodeAffinity) : FixedInstantNode {
    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val args = params.map { evalWithTrace(it, ec, eng) }
        val fn = eng.functions[fnName]
                ?: TODO("inconsistent state: function not found in evaluation (must be resolved in planning phase) $fnName")
        return fn.eval(ec, metPlan, args, params)
    }
    override fun reprNode(): String {
        return "FunctionNode($fnName, fixed)"
    }
}

data class FunctionNode(val fnName: String, override val metPlan: MetricPlan, val params: List<PlanNode>) : InstantNode {
    override val affinity: NodeAffinity = NodeAffinity.Splitted

    override fun reprNode(): String {
        return "FunctionNode($fnName)"
    }

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val args = params.map { evalWithTrace(it, ec, eng) }
        val fn = eng.functions[fnName]
                ?: TODO("inconsistent state: function not found in evaluation (must be resolved in planning phase) $fnName")

        return fn.eval(ec, metPlan, args, params)
    }
}

