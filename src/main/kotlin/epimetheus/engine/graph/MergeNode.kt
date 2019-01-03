package epimetheus.engine.graph

import epimetheus.engine.EngineContext
import epimetheus.engine.ExecContext
import epimetheus.engine.RemoteExec
import epimetheus.engine.plan.FixedMetric
import epimetheus.engine.plan.RPointMatrix
import epimetheus.engine.plan.RRangeMatrix
import epimetheus.engine.plan.RuntimeValue
import epimetheus.pkg.promql.InstantSelector
import epimetheus.storage.IgniteGateway
import kotlin.streams.toList

abstract class MergeNode(val nodes: List<FixedInstantNode>) : FixedInstantNode {
    override val affinity: NodeAffinity = NodeAffinity.Splitted

    override val metPlan: FixedMetric = FixedMetric(nodes.flatMap { it.metPlan.metrics })
}

// selectorHint is used when evaluation 'absent' function
class MergePointNode(nodes: List<FixedInstantNode>, val selectorHint: InstantSelector? = null) : MergeNode(nodes) {
    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val vals = nodes.parallelStream().map {
            val aff = it.affinity
            if (eng.ignite != null && aff is NodeAffinity.Single) {
                val g = eng.gateway as IgniteGateway
                g.affinityCall(aff.metric, RemoteExec(it, ec)) as RPointMatrix
            } else {
                it.evaluate(ec, eng) as RPointMatrix
            }
        }.toList()
        return RPointMatrix.merge(vals, ec.frames)
    }
}

class MergeRangeNode(nodes: List<FixedInstantNode>, val range: Long) : MergeNode(nodes) {
    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val vals = nodes.parallelStream().map {
            val aff = it.affinity
            if (eng.ignite != null && aff is NodeAffinity.Single) {
                val g = eng.gateway as IgniteGateway
                g.affinityCall(aff.metric, RemoteExec(it, ec)) as RRangeMatrix
            } else {
                it.evaluate(ec, eng) as RRangeMatrix
            }
        }.toList()
        return RRangeMatrix.merge(vals, ec.frames, range)
    }
}
