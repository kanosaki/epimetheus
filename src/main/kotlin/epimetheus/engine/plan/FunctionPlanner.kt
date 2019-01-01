package epimetheus.engine.plan

import epimetheus.engine.EngineContext
import epimetheus.engine.graph.PlanNode
import epimetheus.engine.primitive.Function
import epimetheus.pkg.promql.FunctionCall
import epimetheus.pkg.promql.PromQLException

class FunctionPlanner(val binding: Map<String, Function>) {
    fun plan(planner: Planner, ast: FunctionCall, ctx: EngineContext): PlanNode {
        val params = ast.args.map { planner.plan(it, ctx) }
        val fn = binding[ast.fn.name] ?: throw PromQLException("function ${ast.fn.name} not found")
        return fn.plan(params)
    }
}

