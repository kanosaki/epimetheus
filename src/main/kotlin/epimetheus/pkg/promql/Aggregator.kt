package epimetheus.pkg.promql

import epimetheus.engine.EvalNode
import epimetheus.model.*
import it.unimi.dsi.fastutil.doubles.Double2IntArrayMap
import it.unimi.dsi.fastutil.doubles.Double2IntRBTreeMap
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import it.unimi.dsi.fastutil.ints.AbstractIntSet
import it.unimi.dsi.fastutil.ints.IntArraySet
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap
import java.util.*

data class Aggregator(
        val name: String,
        override val argTypes: List<ValueType> = listOf(ValueType.Matrix),
        override val returnType: ValueType = ValueType.Vector,
        override val variadic: Boolean = false,
        val evalFn: (params: List<Value>, args: List<Expression>, node: EvalNode, groups: AggregatorGroup?) -> GridMat
) : Applicative {
    fun call(params: List<Value>, args: List<Expression>, node: EvalNode, groups: AggregatorGroup?): GridMat {
        return evalFn(params, args, node, groups)
    }
    companion object {

        private fun fingerprint(met: Metric, groups: AggregatorGroup?): Long {
            if (groups == null) {
                return met.fingerprint()
            }
            return when (groups.typ) {
                AggregatorGroupType.By -> met.filteredFingerprint(true, groups.labels)
                AggregatorGroupType.Without -> met.filteredFingerprint(false, groups.labels, true)
            }
        }

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
                        GridMat(filteredMets.values.toTypedArray(), m.timestamps, filteredMets.keys.map { metKey ->
                            val values = filteredMetMapping[metKey].map { m.values[it!!] }
                            fn(values)
                        })
                    }
                    else -> GridMat(arrayOf(Metric.empty), m.timestamps, listOf(fn(m.values)))
                }
            }
        }

        private fun topK(k: Int, m: GridMat, g: AggregatorGroup?, compareFn: (Double, Double) -> Int): GridMat {
            val tops = Double2IntRBTreeMap(Comparator(compareFn))
            val buckets = Long2ObjectOpenHashMap<AbstractIntSet>(m.metrics.size)
            val bucketed = g != null
            m.metrics.forEachIndexed { index, met ->
                val bucketId = if (bucketed) fingerprint(met, g) else 0
                if (buckets.containsKey(bucketId)) {
                    buckets[bucketId].add(index)
                } else {
                    val ias: AbstractIntSet = if (bucketed) IntArraySet() else IntOpenHashSet()
                    ias.add(index)
                    buckets[bucketId] = ias
                }
            }

            val ret = GridMat(m.metrics, m.timestamps, m.values.map { it.copyOf() })
            for (tsIdx in 0 until m.timestamps.size) {
                buckets.forEach { _, bucket ->
                    for (metIdx in bucket.iterator()) {
                        val v = m.values[metIdx][tsIdx]
                        if (!v.isFinite()) {
                            continue
                        }
                        if (tops.size < k) {
                            tops[v] = metIdx
                        } else {
                            val first = tops.firstDoubleKey()
                            if (first < v) {
                                tops.remove(first)
                                tops[v] = metIdx
                            }
                        }
                    }
                    val topMets = tops.values
                    for (metIdx in bucket.iterator()) {
                        if (!topMets.contains(metIdx)) {
                            ret.values[metIdx][tsIdx] = Mat.StaleValue
                        }
                    }
                    tops.clear()
                }
            }
            return ret.prune()
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
                Aggregator("stddev", evalFn = simpleColMapping { values ->
                    DoubleArray(values[0].size) {
                        var aux = 0.0
                        var count = 0.0
                        var mean = 0.0
                        for (i in 0 until values.size) {
                            count++
                            val delta = values[i][it] - mean
                            mean += delta / count
                            aux += delta * (values[i][it] - mean)
                        }
                        Math.sqrt(aux / count)
                    }
                }),
                Aggregator("stdvar", evalFn = simpleColMapping { values ->
                    DoubleArray(values[0].size) {
                        var aux = 0.0
                        var count = 0.0
                        var mean = 0.0
                        for (i in 0 until values.size) {
                            count++
                            val delta = values[i][it] - mean
                            mean += delta / count
                            aux += delta * (values[i][it] - mean)
                        }
                        aux / count
                    }
                }),
                Aggregator("topk") { vals, a, n, g ->
                    val kDbl = vals[0] as Scalar
                    val k = kDbl.value.toInt()
                    val m = vals[1] as GridMat
                    val tops = Double2IntArrayMap(Math.min(k, m.values.size))
                    val buckets = Long2ObjectOpenHashMap<AbstractIntSet>(m.metrics.size)
                    val bucketed = g != null
                    m.metrics.forEachIndexed { index, met ->
                        val bucketId = if (bucketed) fingerprint(met, g) else 0
                        if (buckets.containsKey(bucketId)) {
                            buckets[bucketId].add(index)
                        } else {
                            val ias: AbstractIntSet = if (bucketed) IntArraySet() else IntOpenHashSet()
                            ias.add(index)
                            buckets[bucketId] = ias
                        }
                    }

                    val ret = GridMat(m.metrics, m.timestamps, m.values.map { it.copyOf() })
                    for (tsIdx in 0 until m.timestamps.size) {
                        buckets.forEach { _, bucket ->
                            for (metIdx in bucket.iterator()) {
                                val v = m.values[metIdx][tsIdx]
                                if (tops.size < k) {
                                    tops[v] = metIdx
                                } else {
                                    val min = tops.minBy {
                                        if (it.key.isNaN()) Double.NEGATIVE_INFINITY else it.key
                                    }!!
                                    if (min.key < v || min.key.isNaN()) {
                                        tops.remove(min.key)
                                        tops[v] = metIdx
                                    }
                                }
                            }
                            val topMets = tops.values
                            for (metIdx in bucket.iterator()) {
                                if (!topMets.contains(metIdx)) {
                                    ret.values[metIdx][tsIdx] = Mat.StaleValue
                                }
                            }
                            tops.clear()
                        }
                    }
                    ret.prune()
                },
                Aggregator("bottomk") { vals, a, n, g ->
                    val kDbl = vals[0] as Scalar
                    val k = kDbl.value.toInt()
                    val m = vals[1] as GridMat
                    val tops = Double2IntArrayMap(Math.min(k, m.values.size))
                    val buckets = Long2ObjectOpenHashMap<AbstractIntSet>(m.metrics.size)
                    val bucketed = g != null
                    m.metrics.forEachIndexed { index, met ->
                        val bucketId = if (bucketed) fingerprint(met, g) else 0
                        if (buckets.containsKey(bucketId)) {
                            buckets[bucketId].add(index)
                        } else {
                            val ias: AbstractIntSet = if (bucketed) IntArraySet() else IntOpenHashSet()
                            ias.add(index)
                            buckets[bucketId] = ias
                        }
                    }

                    val ret = GridMat(m.metrics, m.timestamps, m.values.map { it.copyOf() })
                    for (tsIdx in 0 until m.timestamps.size) {
                        buckets.forEach { _, bucket ->
                            for (metIdx in bucket.iterator()) {
                                val v = m.values[metIdx][tsIdx]
                                if (tops.size < k) {
                                    tops[v] = metIdx
                                } else {
                                    val max = tops.maxBy { if (it.key.isNaN()) Double.POSITIVE_INFINITY else it.key }!!
                                    if (max.key > v || max.key.isNaN()) {
                                        tops.remove(max.key)
                                        tops[v] = metIdx
                                    }
                                }
                            }
                            val topMets = tops.values
                            for (metIdx in bucket.iterator()) {
                                if (!topMets.contains(metIdx)) {
                                    ret.values[metIdx][tsIdx] = Mat.StaleValue
                                }
                            }
                            tops.clear()
                        }
                    }
                    ret.prune()
                },
                Aggregator("count_values") { vals, _, _, g ->
                    val targetLabel = (vals[0] as StringValue).value
                    val m = vals[1] as GridMat
                    val metCache = Long2ObjectRBTreeMap<Metric>()
                    val counter = mutableMapOf<Pair<Int, Long>, Int>()
                    fun fingerprint(tsIdx: Int, metId: Int): Long {
                        val baseMet = when (g?.typ) {
                            AggregatorGroupType.By -> m.metrics[metId].filterOn(g.labels)
                            AggregatorGroupType.Without -> m.metrics[metId].filterWithout(true, g.labels)
                            else -> Metric.empty
                        }
                        val v = m.values[metId][tsIdx]
                        val mb = baseMet.builder()
                        mb.put(targetLabel, Utils.fmtDouble(v))
                        val met = mb.build()
                        val fp = met.fingerprint()
                        metCache[fp] = met
                        return fp
                    }
                    // scan whole metrics
                    for (tsIdx in 0 until m.timestamps.size) {
                        for (metIdx in 0 until m.metrics.size) {
                            val fp = fingerprint(tsIdx, metIdx)
                            val key = tsIdx to fp
                            if (counter.containsKey(key)) {
                                counter[key] = counter[key]!! + 1
                            } else {
                                counter[key] = 1
                            }
                        }
                    }
                    val metrics = metCache.values.toTypedArray()
                    val values = metrics.map { met ->
                        val fp = met.fingerprint()
                        DoubleArray(m.timestamps.size) { tsIdx ->
                            val ctr = counter[tsIdx to fp]!!
                            ctr.toDouble()
                        }
                    }
                    GridMat(metrics, m.timestamps, values)
                },
                Aggregator("quantile") { vals, a, node, g ->
                    val q = (vals[0] as Scalar).value
                    val m = vals[1] as GridMat
                    simpleColMapping { values ->
                        val dal = DoubleArrayList()
                        DoubleArray(values[0].size) {
                            for (i in 0 until values.size) {
                                if (!Mat.isStale(values[i][it])) {
                                    dal.add(values[i][it])
                                }
                            }
                            dal.trim()
                            val vs = dal.elements()
                            if (vs.isEmpty()) {
                                return@DoubleArray Mat.StaleValue
                            }

                            val ret = Utils.quantile(q, vs)
                            dal.clear()
                            ret
                        }
                    }.invoke(listOf(m), a, node, g)
                }
        )
    }
}
