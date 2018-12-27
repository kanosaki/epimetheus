package epimetheus.engine.plan

import epimetheus.pkg.promql.AggregatorGroup
import epimetheus.pkg.promql.PromQLException

class ColumnMapAggregatorNode(override val metric: FixedMetric, val param: InstantNode, val fnName: String, val grouping: List<IntArray>?) : InstantNode {
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val p = param.evaluate(ec)
        val agg = ec.aggregators[fnName] ?: throw PromQLException("aggregator $fnName not found")
        agg as? MappingAggregator ?: TODO("inconsistent aggregator")
        return agg.eval(ec, metric.metrics, listOf(p), grouping)
    }
}

class ColumnMapAggregatorDynamicNode(val param: InstantNode, val fnName: String, val group: AggregatorGroup?) : InstantNode {
    override val metric: MetricPlan = VariableMetric

    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val p = param.evaluate(ec)
        val pm = p as? RPointMatrix ?: throw PromQLException("instant-vector expected but got ${p.javaClass}")
        val agg = ec.aggregators[fnName] ?: throw PromQLException("aggregator $fnName not found")
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
    override val metric: MetricPlan = VariableMetric
    override fun evaluate(ec: EvaluationContext): RuntimeValue {
        val agg = ec.aggregators[fnName] ?: throw PromQLException("aggregator $fnName not found")
        val args = params.map { it.evaluate(ec) }
        return when (agg) {
            is VariadicAggregator -> {
                agg.fn(ec, args, group)
            }
            else -> TODO("couldn't evaluate mapping aggregator inner VariadicAggregatorNode")
        }
    }
}
