package epimetheus.engine

import epimetheus.model.Value
import epimetheus.pkg.promql.Expression
import java.io.PrintStream
import java.util.*

interface Tracer {
    fun enteringEvalExpr(ast: Expression, depth: Int) {

    }

    fun onEvalExpr(ast: Expression, result: Value, depth: Int) {

    }

    companion object {
        val empty = object : Tracer {}
    }
}

data class EvalLog(val ast: Expression, val elapsedNs: Long, val result: Value, val depth: Int)

class FullLoggingTracer : Tracer {
    private val timerStack = Stack<Long>()
    private var lastDepth = -1

    val log = LinkedList<EvalLog>()

    override fun enteringEvalExpr(ast: Expression, depth: Int) {
        // step in
        if (depth >= lastDepth) {
            timerStack.push(System.nanoTime())
        }
        lastDepth = depth
    }

    override fun onEvalExpr(ast: Expression, result: Value, depth: Int) {
        val finished = System.nanoTime()
        val beginTime = timerStack.pop()
        log.add(EvalLog(ast, finished - beginTime, result, timerStack.size))
    }

    fun printTrace(out: PrintStream = System.out, indent: Int = 2): String {
        val sb = StringBuilder()
        for (entry in log.asReversed()) {
            if (entry.depth != 0) {
                out.print('|')
                out.print(" ".repeat(indent * entry.depth - 1))
            }
            out.println("${entry.ast}")
        }
        for (entry in log) {
            if (entry.depth != 0) {
                out.print('|')
                out.print(" ".repeat(indent * entry.depth - 1))
            }
            out.println("--> ${entry.elapsedNs / 1000}us ${entry.result}")
        }

        return sb.toString()
    }
}
