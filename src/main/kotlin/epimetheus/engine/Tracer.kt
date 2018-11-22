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

data class EvalLog(val ast: Expression, val timestamp: Long, val result: Value, val depth: Int)

class FullLoggingTracer : Tracer {
    private var beginTime: Long? = null
    val log = LinkedList<EvalLog>()

    override fun enteringEvalExpr(ast: Expression, depth: Int) {
        beginTime = System.nanoTime()
    }

    override fun onEvalExpr(ast: Expression, result: Value, depth: Int) {
        try {
            log.add(EvalLog(ast, System.nanoTime(), result, depth))
        } catch (e: EmptyStackException) {
            println("EVAL LOG")
            println(log.joinToString("\n"))
            e.printStackTrace()
        }
    }

    fun printTrace(out: PrintStream = System.out, indent: Int = 2): String {
        if (log.isEmpty()) {
            return "EMPTY LOG"
        }
        val bt = beginTime!!
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
            out.println("--> ${(entry.timestamp - bt) / 1000}us ${entry.result}")
        }

        return sb.toString()
    }
}
