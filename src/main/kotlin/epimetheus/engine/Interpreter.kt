package epimetheus.engine

import epimetheus.model.TimeFrames
import epimetheus.model.Value
import epimetheus.pkg.promql.Expression
import epimetheus.pkg.promql.PromQL
import epimetheus.storage.Gateway
import org.antlr.v4.runtime.CharStreams

class Interpreter(val storage: Gateway) {
    fun eval(query: String, frames: TimeFrames): Value {
        val ast = PromQL.parse(CharStreams.fromString(query))!!
        return evalAst(ast, frames)
    }

    fun evalAst(query: Expression, frames: TimeFrames, tracer: Tracer = Tracer.empty): Value {
        val evalCtx = Eval(frames, storage, tracer)
        return evalCtx.evalExpr(query, 0)
    }
}