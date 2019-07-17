package epimetheus.engine

import epimetheus.engine.plan.Planner
import epimetheus.model.RuntimeValue
import epimetheus.model.TimeFrames
import epimetheus.pkg.promql.Expression
import epimetheus.pkg.promql.PromQL
import epimetheus.storage.Gateway
import epimetheus.storage.IgniteGateway
import org.antlr.v4.runtime.CharStreams

class Engine(val storage: Gateway, private val slowQueryThreshould: Long? = null) {
    val planner = Planner(storage)
    val ctx = EngineContext.builtin(storage, if (storage is IgniteGateway) storage.ignite else null)

    fun exec(query: String, frames: TimeFrames): RuntimeValue {
        val tracer = if (slowQueryThreshould != null) SpanTracer() else NopTracer
        val res = execWithTracer(query, frames, tracer)
        if (slowQueryThreshould != null && tracer.elapsedMs()!! > slowQueryThreshould) { // its a slow query
            println("Traced: ${tracer.elapsedMs()}(ms) $query")
            tracer.printTrace(System.out)
        }
        return res
    }

    fun execWithTracer(query: String, frames: TimeFrames, tracer: Tracer): RuntimeValue {
        tracer.markBegin()
        tracer.onPhase("parse")
        val ast = PromQL.parse(CharStreams.fromString(query))!!
        val ec = ExecContext(frames, tracer)
        val res = evalAst(ast, ec)
        tracer.markEnd()
        return res
    }

    fun evalAst(query: Expression, ec: ExecContext): RuntimeValue {
        ec.tracer.markBegin()
        val executor = Exec(storage, ctx, planner)
        val res = executor.queryRange(query, ec)
        ec.tracer.markEnd()
        return res
    }
}
