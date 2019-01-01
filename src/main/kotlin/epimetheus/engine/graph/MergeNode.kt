package epimetheus.engine.graph

import epimetheus.engine.EngineContext
import epimetheus.engine.ExecContext
import epimetheus.engine.plan.*
import epimetheus.pkg.promql.InstantSelector

abstract class MergeNode(val nodes: List<FixedInstantNode>) : FixedInstantNode {
    override val affinity: NodeAffinity = NodeAffinity.Splitted

    override val metPlan: FixedMetric = FixedMetric(nodes.flatMap { it.metPlan.metrics })
}

// selectorHint is used when evaluation 'absent' function
class MergePointNode(nodes: List<FixedInstantNode>, val selectorHint: InstantSelector? = null) : MergeNode(nodes) {
    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val vals = nodes.map { it.evaluate(ec, eng) as RPointMatrix }
        return RPointMatrix.merge(vals, ec.frames)
    }
}

class MergeRangeNode(nodes: List<FixedInstantNode>, val range: Long) : MergeNode(nodes) {
    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val vals = nodes.map {
            val v = it.evaluate(ec, eng)
            if (v is RPointMatrix) {
                println("D -- $it")
            }
            v as RRangeMatrix
        }
        return RRangeMatrix.merge(vals, ec.frames, range)
    }
}
