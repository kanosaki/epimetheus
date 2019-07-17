package epimetheus.engine

import epimetheus.engine.graph.PlanNode
import epimetheus.engine.graph.RootNode
import epimetheus.model.RPointMatrix
import epimetheus.model.RuntimeValue
import java.io.PrintStream
import java.io.Serializable
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

abstract class Tracer : Serializable {
    open fun onPhase(name: String) {

    }

    open fun printTrace(ps: PrintStream) {

    }

    open fun addTrace(ec: ExecContext, parent: PlanNode, child: PlanNode, result: RuntimeValue, evalBegin: Long, evalEnd: Long) {
    }

    open fun addPlan(ec: ExecContext, rootPlan: PlanNode) {

    }

    var begin: Long? = null
    var end: Long? = null

    open fun markBegin() {
        if (begin == null) {
            begin = System.nanoTime()
        }
    }

    open fun markEnd() {
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

object NopTracer : Tracer() {}

open class SpanTracer : Tracer() {
    val phases = mutableMapOf<String, Long>()
    protected var beginMills: Long? = null
    override fun markBegin() {
        super.markBegin()
        beginMills = System.currentTimeMillis()
    }

    override fun onPhase(name: String) {
        phases[name] = System.nanoTime()
    }

    override fun printTrace(ps: PrintStream) {
        val beginMills = beginMills
        val beginNanos = begin
        if (beginMills == null || beginNanos == null) {
            ps.println("Phases: ERROR: `markBegin` haven't be called")
            return
        }
        val phases = phases.entries.sortedBy { entry -> entry.value }
        ps.println("Phases:")
        ps.println("  BEGIN: ${Instant.ofEpochMilli(beginMills)}")
        var prevTime: Long = beginNanos
        for (phase in phases) {
            ps.println("  ${phase.key}: ${(phase.value - prevTime) / 1000}us")
            prevTime = phase.value
        }
    }
}

class LocalRecordTracer : SpanTracer() {
    data class Record(val result: RuntimeValue, val begin: Long, val end: Long, val ec: ExecContext)

    val resultRecord = ConcurrentHashMap<PlanNode, Record>()
    // `null` key for root node
    val callGraph = ConcurrentHashMap<PlanNode?, MutableList<PlanNode>>()
    var plan: PlanNode? = null

    override fun printTrace(ps: PrintStream) {
        super.printTrace(ps)
        ps.println("EvalRecord(local): ${resultRecord.size} records ${callGraph.size} nodes")
        if (plan != null) {
            ps.println("Plan:")
            ps.println(plan!!.reprRecursive())
        }
        // check root node
        val firstChildren = callGraph[RootNode]
        if (firstChildren == null) {
            ps.println("  ERROR: no root node")
            return
        }
        ps.println("Root")
        for (i in 0 until firstChildren.size) {
            printCallNode(ps, firstChildren[i], "", i == 0, i == firstChildren.size - 1)
        }
    }

    private fun printNodeValue(ps: PrintStream, rec: Record, prefix: String, hasNextSibling: Boolean, hasChildren: Boolean) {
        val res = rec.result
        val heading = "$prefix${if (hasNextSibling) "│" else " "}   ${if (hasChildren) "│" else " "}"
        val valueIndent = "$heading :"
        val rValue = when (res) {
            is RPointMatrix -> res.toTable().printAll().trimEnd().replace("\n", "\n$valueIndent")
            else -> res.toString()
        }
        ps.println("$heading --> $rValue")
        ps.println("$heading └ (${(rec.end - rec.begin) / 1000}us)")
    }

    private fun printCallNode(ps: PrintStream, node: PlanNode, prefix: String, isHead: Boolean, isTail: Boolean) {
        val rec = resultRecord[node]
        val children = callGraph[node]

        if (rec != null) {
            ps.println("$prefix${if (isTail) "└── " else "├── "}${node.reprNode()}: ${rec.ec}")
            printNodeValue(
                    ps,
                    rec,
                    prefix,
                    !isTail,
                    children != null && children.isNotEmpty())
        } else {
            ps.println("$prefix${if (isTail) "└── " else "├── "}${node.reprNode()}")
        }

        if (children != null) {
            children.sortBy { resultRecord[it]!!.begin }
            for (i in 0 until children.size) {
                printCallNode(ps, children[i], prefix + if (isTail) "    " else "│   ", i == 0, i == children.size - 1)
            }
        }
    }

    override fun addTrace(ec: ExecContext, parent: PlanNode, child: PlanNode, result: RuntimeValue, evalBegin: Long, evalEnd: Long) {
        if (end != null) {
            throw RuntimeException("cannot add data to a closed tracer")
        }
        callGraph.compute(parent) { _, prev ->
            if (prev == null) {
                mutableListOf(child)
            } else {
                prev.add(child)
                prev
            }
        }
        resultRecord[child] = Record(result, evalBegin, evalEnd, ec)
    }

    override fun addPlan(ec: ExecContext, rootPlan: PlanNode) {
        this.plan = rootPlan
    }
}



