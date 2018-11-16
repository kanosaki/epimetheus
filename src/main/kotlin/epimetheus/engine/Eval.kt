package epimetheus.engine

import epimetheus.model.Scalar
import epimetheus.model.TimeFrames
import epimetheus.model.Value
import epimetheus.pkg.promql.*
import epimetheus.storage.Gateway

class Eval(val frames: TimeFrames, val storage: Gateway, val tracer: Tracer = Tracer.empty) {
    fun eval(ast: Expression): Value {
        val node = DefaultEvalNode(frames, this, null)
        return node.evalExpr(ast, 0)
    }
}

interface EvalNode {
    val timeScope: TimeFrames
    fun evalExpr(ast: Expression, depth: Int): Value
}


class DefaultEvalNode(override val timeScope: TimeFrames, val top: Eval, val parent: EvalNode?) : EvalNode {
    override fun evalExpr(ast: Expression, depth: Int): Value {
        top.tracer.enteringEvalExpr(ast, depth)
        // TODO: optimize
        val res = when (ast) {
            is NumberLiteral -> Scalar(ast.value)
            is InstantSelector -> {
                top.storage.collectInstant(ast.matcher, timeScope)
            }
            is MatrixSelector -> {
                val rangeMs = ast.range.toMillis()
                top.storage.collectRange(ast.matcher, timeScope, rangeMs, 0)
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
                val childNode = DefaultEvalNode(timeScope, top, this)
                val params = ast.params.map { childNode.evalExpr(it, depth + 1) }
                ast.fn.call(params, this)
            }
            else -> TODO("$ast not implemented")
        }
        top.tracer.onEvalExpr(ast, res, depth)
        return res
    }
}