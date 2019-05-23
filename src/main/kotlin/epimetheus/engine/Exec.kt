package epimetheus.engine

import epimetheus.engine.graph.RootNode
import epimetheus.engine.plan.Planner
import epimetheus.engine.plan.RPointMatrix
import epimetheus.engine.plan.RuntimeValue
import epimetheus.pkg.promql.Expression
import epimetheus.pkg.promql.PromQLException
import epimetheus.storage.Gateway

class Exec(val gateway: Gateway, val eng: EngineContext, val planner: Planner) {
    fun queryRange(ast: Expression, ec: ExecContext): RuntimeValue {
        ec.tracer.onPhase("plan")
        val plan = planner.plan(ast, eng)
        ec.tracer.addPlan(ec, plan)
        ec.tracer.onPhase("exec")

        val begin = System.nanoTime()
        val res = plan.evaluate(ec, eng)
        val end = System.nanoTime()
        ec.tracer.addTrace(ec, RootNode, plan, res, begin, end)

        return if (res is RPointMatrix) {
            res.sortSeries()
            for (i in 1 until res.metrics.size) {
                if (res.metrics[i - 1].fingerprint() == res.metrics[i].fingerprint()) {
                    throw PromQLException("duplicated label set: ${res.metrics[i]}")
                }
            }
            res
        } else {
            res
        }
    }
}
