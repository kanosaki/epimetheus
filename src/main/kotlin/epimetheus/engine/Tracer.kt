package epimetheus.engine

import epimetheus.model.Value
import epimetheus.pkg.promql.Expression
import java.io.PrintStream
import java.util.*

abstract class Tracer {
    open fun enteringEvalExpr(ast: Expression, depth: Int) {

    }

    open fun onEvalExpr(ast: Expression, result: Value, depth: Int) {

    }

    open fun onPhase(name: String) {

    }

    open fun printTrace(out: PrintStream = System.out, indent: Int = 2): String {
        return "elapsed: ${this.elapsedMs()}"
    }

    companion object {
        val empty = object : Tracer() {}
    }

    protected var begin: Long? = null
    protected var end: Long? = null

    fun markBegin() {
        begin = System.nanoTime()
    }

    fun markEnd() {
        end = System.nanoTime()
    }

    fun elapsedMs(): Long? {
        return if (end != null && begin != null) {
            (end!! - begin!!) / 1000 / 1000
        } else {
            null
        }
    }
}

class PhaseTracer : Tracer() {
    val phases = mutableMapOf<String, Long>()
    override fun onPhase(name: String) {
        synchronized(phases) {
            phases[name] = System.nanoTime()
        }
    }

    fun endTime(): Long? {
        return end
    }
}

data class EvalLog(val ast: Expression, val timestamp: Long, val result: Value, val depth: Int)

data class EventTimeLog(val ast: Expression, val timestamp: Long, val depth: Int)

class TimingTracer : Tracer() {
    private var beginTime: Long? = null
    val log = LinkedList<EventTimeLog>()
    override fun enteringEvalExpr(ast: Expression, depth: Int) {
        if (beginTime == null) {
            beginTime = System.nanoTime()
        }
    }

    override fun onEvalExpr(ast: Expression, result: Value, depth: Int) {
        try {
            log.add(EventTimeLog(ast, System.nanoTime(), depth))
        } catch (e: EmptyStackException) {
            println("EVAL LOG")
            println(log.joinToString("\n"))
            e.printStackTrace()
        }
    }

    override fun printTrace(out: PrintStream, indent: Int): String {
        val elapsed = if (begin != null && end != null) (end!! - begin!!) else null
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
            val spanTime = entry.timestamp - bt
            if (elapsed != null) {
                out.println("--> ${(entry.timestamp - bt) / 1000 / 1000}ms (${spanTime.toDouble() * 100.0 / elapsed.toDouble()}%)")
            } else {
                out.println("--> ${(entry.timestamp - bt) / 1000 / 1000}ms")
            }
        }

        return sb.toString()
    }
}

class FullLoggingTracer : Tracer() {
    private var beginTime: Long? = null
    val log = LinkedList<EvalLog>()

    override fun enteringEvalExpr(ast: Expression, depth: Int) {
        if (beginTime == null) {
            beginTime = System.nanoTime()
        }
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

    override fun printTrace(out: PrintStream, indent: Int): String {
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
