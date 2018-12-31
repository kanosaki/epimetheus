package epimetheus.engine

import epimetheus.engine.plan.*
import epimetheus.engine.plan.Function
import epimetheus.model.TimeFrames
import epimetheus.pkg.promql.Expression
import epimetheus.storage.Gateway

class Exec(val gateway: Gateway, val planner: Planner) {
    fun queryRange(ast: Expression, frames: TimeFrames): RuntimeValue {
        val plan = planner.plan(ast, PlannerContext())
        val ec = EvaluationContext(frames, gateway, Aggregator.builtins, Function.builtins)
        val res = plan.evaluate(ec)
        return if (res is RPointMatrix) {
            res.sortSeries()
        } else {
            res
        }
    }
}