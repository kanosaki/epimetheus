package epimetheus.engine

import epimetheus.model.Scalar
import epimetheus.model.TimeFrames
import epimetheus.model.Value
import epimetheus.pkg.promql.*
import epimetheus.storage.Gateway
import java.util.*

class Eval(val frames: TimeFrames, val storage: Gateway, val tracer: Tracer = Tracer.empty) {
    fun eval(ast: Expression): Value {
        val node = DefaultEvalNode(this, null)
        return node.evalExpr(ast, 0)
    }
}

// EvalNodeHelper at Prometheus
interface EvalNode {
    val top: Eval

    fun evalExpr(ast: Expression, depth: Int): Value
    fun locale(): Calendar {
        return Calendar.getInstance()
    }

    val frames: TimeFrames
        get() = top.frames
}


class DefaultEvalNode(override val top: Eval, val parent: EvalNode?) : EvalNode {
    override fun evalExpr(ast: Expression, depth: Int): Value {
        top.tracer.enteringEvalExpr(ast, depth)
        // TODO: optimize
        val res = when (ast) {
            is NumberLiteral -> Scalar(ast.value)
            is InstantSelector -> {
                top.storage.collectInstant(ast.matcher, top.frames, ast.offset.toMillis())
            }
            is MatrixSelector -> {
                val rangeMs = ast.range.toMillis()
                top.storage.collectRange(ast.matcher, top.frames, rangeMs, ast.offset.toMillis())
            }
            is BinaryCall -> {
                val evLhs = evalExpr(ast.lhs, depth + 1)
                val evRhs = evalExpr(ast.rhs, depth + 1)
                // XXX: identical mapping
                ast.op.eval(evLhs, evRhs) // TODO
            }
            is AggregatorCall -> {
                val vals = ast.params.map { evalExpr(it, depth + 1) }
                ast.agg.evalFn(vals, ast.params, this, ast.groups)
            }
            is FunctionCall -> {
                val childNode = DefaultEvalNode(top, this)
                val params = ast.args.map { childNode.evalExpr(it, depth + 1) }
                ast.fn.call(params, ast.args, this)
            }
            else -> TODO("$ast not implemented")
        }
        top.tracer.onEvalExpr(ast, res, depth)
        return res
    }
}