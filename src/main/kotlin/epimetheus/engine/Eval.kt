package epimetheus.engine

import epimetheus.model.Scalar
import epimetheus.model.TimeFrames
import epimetheus.model.Value
import epimetheus.pkg.promql.*
import epimetheus.storage.Gateway

class Eval(val frames: TimeFrames, val storage: Gateway, val tracer: Tracer = Tracer.empty) {
    fun evalExpr(ast: Expression, depth: Int): Value {
        tracer.enteringEvalExpr(ast, depth)
        // TODO: optimize
        val res = when (ast) {
            is NumberLiteral -> Scalar(ast.value)
            is InstantSelector -> {
                storage.collect(ast.matcher, frames)
            }
            is MatrixSelector -> {
                //storage.collect(ast.matcher, frames.stretchStart(ast.range.toMillis()))
                storage.collect(ast.matcher, frames)
            }
            is BinaryCall -> {
                val evLhs = evalExpr(ast.lhs, depth + 1)
                val evRhs = evalExpr(ast.rhs, depth + 1)
                // XXX: identical mapping
                ast.op.eval(evLhs, evRhs) // TODO
            }
            is AggregatorCall -> {
                val params = ast.params.map { evalExpr(it, depth + 1) }
                ast.agg.evalFn(params, ast.groups)
            }
            is FunctionCall -> {
                val params = ast.params.map { evalExpr(it, depth + 1) }
                ast.fn.call(params)
            }
            else -> TODO("$ast not implemented")
        }
        tracer.onEvalExpr(ast, res, depth)
        return res
    }
}