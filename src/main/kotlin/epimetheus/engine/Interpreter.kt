package epimetheus.engine

import epimetheus.model.TimeFrames
import epimetheus.model.Value
import epimetheus.pkg.promql.Expression
import epimetheus.pkg.promql.PromQL
import epimetheus.storage.Gateway
import org.antlr.v4.runtime.CharStreams

class Interpreter(val storage: Gateway, private val slowQueryThreshould: Long? = null) {
    fun eval(query: String, frames: TimeFrames): Value {
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

    fun evalAst(query: Expression, frames: TimeFrames, tracer: Tracer = Tracer.empty): Value {
        val evalCtx = Eval(frames, storage, tracer)
        return evalCtx.eval(query)
    }
}