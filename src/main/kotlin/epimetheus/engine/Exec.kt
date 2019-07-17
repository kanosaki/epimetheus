package epimetheus.engine

import epimetheus.engine.graph.RootNode
import epimetheus.engine.plan.Planner
import epimetheus.model.RPointMatrix
import epimetheus.model.RuntimeValue
import epimetheus.pkg.promql.Expression
import epimetheus.pkg.promql.PromQLException
import epimetheus.storage.Gateway
import it.unimi.dsi.fastutil.longs.LongOpenHashSet

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
            // check duplication
            val fingerprints = LongOpenHashSet(res.metrics.size)
            for (met in res.metrics) {
                val fp = met.fingerprint()
                if (fingerprints.contains(fp)) {
                    throw PromQLException("duplicated label set: $met")
                }
            }
            res
        } else {
            res
        }
    }
}
