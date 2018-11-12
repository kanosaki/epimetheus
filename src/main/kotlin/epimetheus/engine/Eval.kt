package epimetheus.engine

import epimetheus.model.Scalar
import epimetheus.model.TimeFrames
import epimetheus.model.Value
import epimetheus.model.WindowedMat
import epimetheus.pkg.promql.*
import epimetheus.storage.Gateway

class Eval(val frames: TimeFrames, val storage: Gateway, val tracer: Tracer = Tracer.empty) {
    fun eval(ast: Expression): Value {
        val node = DefaultEvalNode(frames, false, this, null)
        return node.evalExpr(ast, 0)
    }
}

interface EvalNode {
    val timeScope: TimeFrames
    fun evalExpr(ast: Expression, depth: Int): Value
}


class DefaultEvalNode(override val timeScope: TimeFrames, val noGrid: Boolean, val top: Eval, val parent: EvalNode?) : EvalNode {
    override fun evalExpr(ast: Expression, depth: Int): Value {
        top.tracer.enteringEvalExpr(ast, depth)
        // TODO: optimize
        val res = when (ast) {
            is NumberLiteral -> Scalar(ast.value)
            is InstantSelector -> {
                top.storage.collectGrid(ast.matcher, timeScope)
            }
            is MatrixSelector -> {
                val rangeMs = ast.range.toMillis()
                val collectRange = timeScope.stretchStart(rangeMs)
                val windowRange = TimeFrames(timeScope.start, timeScope.end + 1, timeScope.step)
                if (noGrid) {
                    WindowedMat(rangeMs, top.storage.collectSeries(ast.matcher, collectRange), windowRange)
                } else {
                    WindowedMat(rangeMs, top.storage.collectGrid(ast.matcher, collectRange), windowRange)
                }
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
                val childNode = DefaultEvalNode(timeScope, ast.fn.noGrid, top, this)
                val params = ast.params.map { childNode.evalExpr(it, depth + 1) }
                ast.fn.call(params, this)
            }
            else -> TODO("$ast not implemented")
        }
        top.tracer.onEvalExpr(ast, res, depth)
        return res
    }
}