package epimetheus.engine.plan

import epimetheus.pkg.promql.FunctionCall
import epimetheus.pkg.promql.PromQLException

class FunctionPlanner(val binding: Map<String, Function>) {
    fun plan(planner: Planner, ast: FunctionCall, ctx: PlannerContext): PlanNode {
        val params = ast.args.map { planner.plan(it, ctx) }
        val fn = binding[ast.fn.name] ?: throw PromQLException("function ${ast.fn.name} not found")
        return fn.plan(params)
    }
}

