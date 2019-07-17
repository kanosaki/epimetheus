package epimetheus.engine.graph

import epimetheus.engine.EngineContext
import epimetheus.engine.ExecContext
import epimetheus.engine.RemoteExec
import epimetheus.engine.plan.FixedMetric
import epimetheus.model.RPointMatrix
import epimetheus.model.RRangeMatrix
import epimetheus.model.RuntimeValue
import epimetheus.pkg.promql.InstantSelector
import epimetheus.storage.IgniteGateway
import kotlin.streams.toList

abstract class MergeNode() : FixedInstantNode {
    abstract val nodes: List<FixedInstantNode>
    override val affinity: NodeAffinity = NodeAffinity.Splitted

    override val metPlan: FixedMetric
        get() = FixedMetric(nodes.flatMap { it.metPlan.metrics })

    override fun reprNode(): String {
        return "${this.javaClass.simpleName}(${if(affinity is NodeAffinity.Single) "uni" else "broad"})"
    }
}

// selectorHint is used when evaluation 'absent' function
data class MergePointNode(override val nodes: List<FixedInstantNode>, val selectorHint: InstantSelector? = null) : MergeNode() {
    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val vals = nodes.parallelStream().map {
            val aff = it.affinity
            if (eng.ignite != null && aff is NodeAffinity.Single) {
                val g = eng.gateway as IgniteGateway
                g.affinityCall(aff.metric, RemoteExec(it, ec)) as RPointMatrix
            } else {
                evalWithTrace(it, ec, eng) as RPointMatrix
            }
        }.toList()
        return RPointMatrix.merge(vals, ec.frames)
    }
}

data class MergeRangeNode(override val nodes: List<FixedInstantNode>, val range: Long) : MergeNode() {
    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val vals = nodes.parallelStream().map {
            val aff = it.affinity
            if (eng.ignite != null && aff is NodeAffinity.Single) {
                val g = eng.gateway as IgniteGateway
                g.affinityCall(aff.metric, RemoteExec(it, ec)) as RRangeMatrix
            } else {
                evalWithTrace(it, ec, eng) as RRangeMatrix
            }
        }.toList()
        return RRangeMatrix.merge(vals, ec.frames, range)
    }
}
