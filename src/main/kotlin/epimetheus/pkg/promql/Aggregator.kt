package epimetheus.pkg.promql

import epimetheus.engine.EvalNode
import epimetheus.model.GridMat
import epimetheus.model.Metric
import epimetheus.model.Value
import it.unimi.dsi.fastutil.ints.IntArraySet
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap

data class Aggregator(
        val name: String,
        override val argTypes: List<ValueType> = listOf(ValueType.Matrix),
        override val returnType: ValueType = ValueType.Vector,
        override val variadic: Boolean = false,
        val evalFn: (params: List<Value>, args: List<Expression>, node: EvalNode, groups: AggregatorGroup?) -> GridMat
) : Applicative {
    companion object {

        private fun simpleColMapping(fn: (List<DoubleArray>) -> DoubleArray): (params: List<Value>, args: List<Expression>, node: EvalNode, groups: AggregatorGroup?) -> GridMat {
            return { param, args, node, group ->
                val m = param[0] as GridMat
                when {
                    m.metrics.isEmpty() -> GridMat(arrayOf(), node.frames, listOf())
                    group != null -> {
                        // TODO: tell MetricRegistry about new metrics
                        val filteredMets = Long2ObjectRBTreeMap<Metric>()
                        val filteredMetMapping = Long2ObjectOpenHashMap<IntArraySet>(m.metrics.size)
                        m.metrics.forEachIndexed { index, met ->
                            val filteredMetric = when (group.typ) {
                                AggregatorGroupType.By -> met.filterOn(*group.labels.toTypedArray())
                                AggregatorGroupType.Without -> met.filterWithout(true, *group.labels.toTypedArray())
                            }
                            val fp = filteredMetric.fingerprint()
                            if (filteredMetMapping.containsKey(fp)) {
                                filteredMetMapping[fp].add(index)
                                filteredMets[fp] = filteredMetric
                            } else {
                                filteredMetMapping[fp] = IntArraySet(listOf(index))
                            }
                        }
                        GridMat(filteredMets.values.toTypedArray(), m.timestamps, filteredMets.keys.map { metKey ->
                            val values = filteredMetMapping[metKey].map { m.values[it!!] }
                            fn(values)
                        })
                    }
                    else -> GridMat(arrayOf(Metric.empty), m.timestamps, listOf(fn(m.values)))
                }
            }
        }

        val builtins = listOf(
                Aggregator("sum", evalFn = simpleColMapping { values ->
                    val sz = values[0].size
                    DoubleArray(sz) {
                        var sum = 0.0
                        for (i in 0 until values.size) {
                            sum += values[i][it]
                        }
                        sum
                    }
                }),
                Aggregator("avg", evalFn = simpleColMapping { values ->
                    DoubleArray(values[0].size) {
                        var sum = 0.0
                        for (i in 0 until values.size) {
                            sum += values[i][it]
                        }
                        sum / values.size
                    }
                }),
                Aggregator("count", evalFn = simpleColMapping { values ->
                    val sz = values[0].size
                    DoubleArray(sz) {
                        var ctr = 0
                        for (i in 0 until values.size) {
                            ctr++
                        }
                        ctr.toDouble()
                    }
                }),
                Aggregator("min", evalFn = simpleColMapping { values ->
                    DoubleArray(values[0].size) {
                        var min = values[0][0]
                        for (i in 0 until values.size) {
                            val v = values[i][it]
                            if (v < min) {
                                min = v
                            }
                        }
                        min
                    }
                }),
                Aggregator("max", evalFn = simpleColMapping { values ->
                    DoubleArray(values[0].size) {
                        var max = values[0][0]
                        for (i in 0 until values.size) {
                            val v = values[i][it]
                            if (v > max) {
                                max = v
                            }
                        }
                        max
                    }
                }),
                Aggregator("stddev") { p, a, n, g -> TODO() },
                Aggregator("stdvar") { p, a, n, g -> TODO() },
                Aggregator("topk") { p, a, n, g -> TODO() },
                Aggregator("bottomk") { p, a, n, g -> TODO() },
                Aggregator("count_values") { p, a, n, g -> TODO() },
                Aggregator("quantile") { p, a, n, g -> TODO() }
        )
    }
}
