package epimetheus.engine.graph

import epimetheus.engine.EngineContext
import epimetheus.engine.ExecContext
import epimetheus.engine.plan.*
import epimetheus.engine.primitive.MappingAggregator
import epimetheus.engine.primitive.VariadicAggregator
import epimetheus.model.RPointMatrix
import epimetheus.model.RuntimeValue
import epimetheus.pkg.promql.AggregatorGroup
import epimetheus.pkg.promql.PromQLException

data class ColumnMapAggregatorNode(override val metPlan: FixedMetric, val param: InstantNode, val fnName: String, val grouping: List<IntArray>?) : FixedInstantNode {
    override val affinity: NodeAffinity = NodeAffinity.Splitted

    override fun reprNode(): String {
        return "$fnName(col-static)"
    }

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val p = evalWithTrace(param, ec, eng)
        val agg = eng.aggregators[fnName] ?: throw PromQLException("aggregator $fnName not found")
        agg as? MappingAggregator ?: TODO("inconsistent aggregator")
        return agg.eval(ec, metPlan.metrics, listOf(p), grouping)
    }
}

class ColumnMapAggregatorDynamicNode(val param: InstantNode, val fnName: String, val group: AggregatorGroup?) : InstantNode {
    override val metPlan: MetricPlan = VariableMetric
    override val affinity: NodeAffinity = NodeAffinity.Splitted

    override fun reprNode(): String {
        return "$fnName(col-dynamic, group=$group)"
    }

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val p = evalWithTrace(param, ec, eng)
        val pm = p as? RPointMatrix ?: throw PromQLException("instant-vector expected but got ${p.javaClass}")
        val agg = eng.aggregators[fnName] ?: throw PromQLException("aggregator $fnName not found")
        agg as? MappingAggregator ?: TODO("inconsistent aggregator")
        return if (group == null) {
            agg.eval(ec, pm.metrics, listOf(pm), null)
        } else {
            val metAndGrouping = AggregatorPlanner.computeMetricsAndGrouping(pm.metrics, group)
            agg.eval(ec, metAndGrouping.first, listOf(pm), metAndGrouping.second)
        }
    }
}

class VariadicAggregatorNode(val params: List<PlanNode>, val fnName: String, val group: AggregatorGroup?) : InstantNode {
    override val metPlan: MetricPlan = VariableMetric
    override val affinity: NodeAffinity = NodeAffinity.Splitted

    override fun reprNode(): String {
        return "$fnName(variadic, group=$group)"
    }

    override fun evaluate(ec: ExecContext, eng: EngineContext): RuntimeValue {
        val agg = eng.aggregators[fnName] ?: throw PromQLException("aggregator $fnName not found")
        val args = params.map { evalWithTrace(it, ec, eng) }
        return when (agg) {
            is VariadicAggregator -> {
                agg.eval(ec, args, group)
            }
            else -> TODO("couldn't evaluate mapping aggregator inner VariadicAggregatorNode")
        }
    }
}
