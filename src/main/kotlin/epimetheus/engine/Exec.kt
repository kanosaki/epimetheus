package epimetheus.engine

import epimetheus.engine.plan.*
import epimetheus.model.TimeFrames
import epimetheus.pkg.promql.Expression
import epimetheus.storage.Gateway

class Exec(val gateway: Gateway, val ctx: EngineContext, val planner: Planner) {
    fun queryRange(ast: Expression, frames: TimeFrames): RuntimeValue {
        val plan = planner.plan(ast, ctx)
        val ec = ExecContext(frames)
        val res = plan.evaluate(ec, ctx)
        return if (res is RPointMatrix) {
            res.sortSeries()
        } else {
            res
        }
    }
}