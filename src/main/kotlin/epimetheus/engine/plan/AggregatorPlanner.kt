package epimetheus.engine.plan

import epimetheus.engine.EngineContext
import epimetheus.engine.graph.*
import epimetheus.engine.primitive.Aggregator
import epimetheus.engine.primitive.MappingAggregator
import epimetheus.engine.primitive.VariadicAggregator
import epimetheus.model.Metric
import epimetheus.pkg.promql.AggregatorCall
import epimetheus.pkg.promql.AggregatorGroup
import epimetheus.pkg.promql.AggregatorGroupType
import epimetheus.pkg.promql.PromQLException
import it.unimi.dsi.fastutil.ints.IntArraySet
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap

class AggregatorPlanner(val binding: Map<String, Aggregator>) {
    fun plan(planner: Planner, ast: AggregatorCall, ctx: EngineContext): InstantNode {
        val fn = binding[ast.agg.name] ?: throw PromQLException("aggregator ${ast.agg.name} not found")
        val params = ast.params.map { planner.plan(it, ctx) }
        return when (fn) {
            is MappingAggregator -> {
                planMappingAggregation(params, ast, ctx)
            }
            is VariadicAggregator -> {
                VariadicAggregatorNode(params, fn.name, ast.groups)
            }
            else -> TODO("never here")
        }
    }

    companion object {
        fun computeMetricsAndGrouping(metrics: List<Metric>,group: AggregatorGroup): Pair<List<Metric>, List<IntArray>> {
            val filteredMets = Long2ObjectRBTreeMap<Metric>()
            val filteredMetMapping = Long2ObjectOpenHashMap<IntArraySet>(metrics.size)
            metrics.forEachIndexed { index, met ->
                val filteredMetric = when (group.typ) {
                    AggregatorGroupType.By -> met.filterOn(group.labels)
                    AggregatorGroupType.Without -> met.filterWithout(true, group.labels)
                }
                val fp = filteredMetric.fingerprint()
                filteredMets[fp] = filteredMetric
                if (filteredMetMapping.containsKey(fp)) {
                    filteredMetMapping[fp].add(index)
                } else {
                    filteredMetMapping[fp] = IntArraySet(listOf(index))
                }
            }
            return filteredMets.values.toList() to filteredMets.keys.map { filteredMetMapping[it]!!.toIntArray() }
        }
    }

    private fun planMappingAggregation(params: List<PlanNode>, ast: AggregatorCall, ctx: EngineContext): InstantNode {
        if (params.size != 1) {
            throw PromQLException("only 1 parameter expected")
        }
        val param = params[0] as? InstantNode ?: throw PromQLException("type error: instant-vector expected")
        val mp = param.metPlan as? FixedMetric
                ?: return ColumnMapAggregatorDynamicNode(param, ast.agg.name, ast.groups)

        val group = ast.groups
        return if (group == null) {
            ColumnMapAggregatorNode(FixedMetric(listOf(Metric.empty)), param as FixedInstantNode, ast.agg.name, null)
        } else {
            val metAndGrouping = computeMetricsAndGrouping(mp.metrics, group)
            return ColumnMapAggregatorNode(FixedMetric(metAndGrouping.first), param, ast.agg.name, metAndGrouping.second)
        }
    }
}
