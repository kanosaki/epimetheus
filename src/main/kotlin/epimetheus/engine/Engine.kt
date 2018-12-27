package epimetheus.engine

import epimetheus.engine.plan.Planner
import epimetheus.engine.plan.RuntimeValue
import epimetheus.model.TimeFrames
import epimetheus.pkg.promql.Expression
import epimetheus.pkg.promql.PromQL
import epimetheus.storage.Gateway
import org.antlr.v4.runtime.CharStreams

class Engine(val storage: Gateway, private val slowQueryThreshould: Long? = null) {
    val planner = Planner(storage)

    fun exec(query: String, frames: TimeFrames): RuntimeValue {
        val ast = PromQL.parse(CharStreams.fromString(query))!!
        val tracer = if (slowQueryThreshould != null) TimingTracer() else Tracer.empty
        tracer.markBegin()
        val res = evalAst(ast, frames, tracer)
        tracer.markEnd()
        if (slowQueryThreshould != null && tracer.elapsedMs()!! > slowQueryThreshould) { // its a slow query
            println("Traced: ${tracer.elapsedMs()}(ms) $query")
            tracer.printTrace()
        }
        return res
    }

    fun evalAst(query: Expression, frames: TimeFrames, tracer: Tracer = Tracer.empty): RuntimeValue {
        val executor = Exec(storage, planner)
        return executor.queryRange(query, frames)
    }
}
