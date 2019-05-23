package epimetheus.engine.primitive

import epimetheus.engine.ExecContext
import epimetheus.engine.plan.*
import epimetheus.model.DoubleSlice
import epimetheus.model.LongSlice
import epimetheus.model.Mat
import epimetheus.model.Metric
import epimetheus.pkg.promql.AggregatorGroup
import epimetheus.pkg.promql.AggregatorGroupType
import epimetheus.pkg.promql.PromQLException
import epimetheus.pkg.promql.Utils
import it.unimi.dsi.fastutil.doubles.Double2IntArrayMap
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import it.unimi.dsi.fastutil.ints.AbstractIntSet
import it.unimi.dsi.fastutil.ints.IntArraySet
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap

interface Aggregator {
    val name: String

    companion object {
        val mappingAggregators = listOf(
                MappingAggregator("sum") { values ->
                    DoubleSlice.init(values[0].size) {
                        var sum = 0.0
                        for (i in 0 until values.size) {
                            sum += values[i][it]
                        }
                        sum
                    }
                },
                MappingAggregator("avg") { values ->
                    DoubleSlice.init(values[0].size) {
                        var sum = 0.0
                        for (i in 0 until values.size) {
                            sum += values[i][it]
                        }
                        sum / values.size
                    }
                },
                MappingAggregator("count") { values ->
                    DoubleSlice.init(values[0].size) {
                        var ctr = 0
                        for (i in 0 until values.size) {
                            if (!Mat.isStale(values[i][it])){
                                ctr++
                            }
                        }
                        ctr.toDouble()
                    }
                },
                MappingAggregator("min") { values ->
                    DoubleSlice.init(values[0].size) {
                        var min = values[0][0]
                        for (i in 0 until values.size) {
                            val v = values[i][it]
                            if (v < min) {
                                min = v
                            }
                        }
                        min
                    }
                },
                MappingAggregator("max") { values ->
                    DoubleSlice.init(values[0].size) {
                        var max = values[0][0]
                        for (i in 0 until values.size) {
                            val v = values[i][it]
                            if (v > max) {
                                max = v
                            }
                        }
                        max
                    }
                },
                MappingAggregator("stddev") { values ->
                    DoubleSlice.init(values[0].size) {
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
                },
                MappingAggregator("stdvar") { values ->
                    DoubleSlice.init(values[0].size) {
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
                }
        )

        private fun fingerprint(met: Metric, groups: AggregatorGroup?): Long {
            if (groups == null) {
                return met.fingerprint()
            }
            return when (groups.typ) {
                AggregatorGroupType.By -> met.filteredFingerprint(true, groups.labels)
                AggregatorGroupType.Without -> met.filteredFingerprint(false, groups.labels, true)
            }
        }

        private val variadicAggregator = listOf(
                VariadicAggregator("topk") { ec, args, group ->
                    val ks = args[0] as RScalar
                    val m = args[1] as RPointMatrix
                    val tops = Double2IntArrayMap(m.rowCount)
                    val buckets = Long2ObjectOpenHashMap<AbstractIntSet>(m.metrics.size)
                    val bucketed = group != null
                    m.metrics.forEachIndexed { index, met ->
                        val bucketId = if (bucketed) fingerprint(met, group) else 0
                        if (buckets.containsKey(bucketId)) {
                            buckets[bucketId].add(index)
                        } else {
                            val ias: AbstractIntSet = if (bucketed) IntArraySet() else IntOpenHashSet()
                            ias.add(index)
                            buckets[bucketId] = ias
                        }
                    }

                    val ret = m.duplicate()
                    for (tsIdx in 0 until m.colCount) {
                        val k = ks.at(tsIdx)
                        if (k < 1.0) {
                            continue
                        }
                        buckets.forEach { (_, bucket) ->
                            for (metIdx in bucket.iterator()) {
                                val v = m.series[metIdx].values[tsIdx]
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
                                    // TODO: fix this dirty hack
                                    ret.series[metIdx].values.write(tsIdx, Mat.StaleValue)
                                }
                            }
                            tops.clear()
                        }
                    }
                    ret.sortSeriesByLastValue(true)
                },
                VariadicAggregator("bottomk") { ec, args, group ->
                    val k = args[0] as RScalar
                    val m = args[1] as RPointMatrix
                    val tops = Double2IntArrayMap(m.rowCount)
                    val buckets = Long2ObjectOpenHashMap<AbstractIntSet>(m.metrics.size)
                    val bucketed = group != null
                    m.metrics.forEachIndexed { index, met ->
                        val bucketId = if (bucketed) fingerprint(met, group) else 0
                        if (buckets.containsKey(bucketId)) {
                            buckets[bucketId].add(index)
                        } else {
                            val ias: AbstractIntSet = if (bucketed) IntArraySet() else IntOpenHashSet()
                            ias.add(index)
                            buckets[bucketId] = ias
                        }
                    }

                    val ret = m.duplicate()
                    for (tsIdx in 0 until m.colCount) {
                        buckets.forEach { (_, bucket) ->
                            for (metIdx in bucket.iterator()) {
                                val v = m.series[metIdx].values[tsIdx]
                                if (tops.size < k.at(tsIdx)) {
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
                                    ret.series[metIdx].values.write(tsIdx, Mat.StaleValue)
                                }
                            }
                            tops.clear()
                        }
                    }
                    ret.sortSeriesByLastValue(false)
                },
                VariadicAggregator("count_values") { ec, args, group ->
                    val targetLabel = (args[0] as RString).value
                    val m = args[1] as RPointMatrix
                    val metCache = Long2ObjectRBTreeMap<Metric>()
                    val counter = mutableMapOf<Pair<Int, Long>, Int>()
                    fun fingerprint(tsIdx: Int, metId: Int): Long {
                        val baseMet = when (group?.typ) {
                            AggregatorGroupType.By -> m.metrics[metId].filterOn(group.labels)
                            AggregatorGroupType.Without -> m.metrics[metId].filterWithout(true, group.labels)
                            else -> Metric.empty
                        }
                        val v = m.series[metId].values[tsIdx]
                        val mb = baseMet.builder()
                        mb.put(targetLabel, Utils.fmtDouble(v))
                        val met = mb.build()
                        val fp = met.fingerprint()
                        metCache[fp] = met
                        return fp
                    }
                    // scan whole metrics
                    for (tsIdx in 0 until m.colCount) {
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
                    val metrics = metCache.values.toList()
                    val timestamps = LongSlice.wrap(ec.frames.toLongArray())
                    val series = metrics.map { met ->
                        val fp = met.fingerprint()
                        val vals = DoubleSlice.init(m.colCount) { tsIdx ->
                            val ctr = counter[tsIdx to fp]!!
                            ctr.toDouble()
                        }
                        RPoints(timestamps, vals)
                    }
                    RPointMatrix(metrics, series, ec.frames)
                },
                VariadicAggregator("quantile") { ec, args, group ->
                    val q = args[0] as RScalar
                    val pm = args[1] as RPointMatrix
                    val timestamps = LongSlice.wrap(ec.frames.toLongArray())
                    fun quantileFn(values: List<DoubleSlice>): DoubleSlice {
                        val dal = DoubleArrayList(values.size)
                        return DoubleSlice.init(values[0].size) { tsIdx ->
                            for (metIdx in 0 until values.size) {
                                val v = values[metIdx][tsIdx]
                                if (!Mat.isStale(v)) {
                                    dal.add(v)
                                }
                            }
                            if (dal.isEmpty) {
                                return@init Mat.StaleValue
                            }
                            dal.trim()
                            val ret = Utils.quantile(q.at(tsIdx), dal.elements())
                            dal.clear()
                            ret
                        }
                    }
                    if (group == null) {
                        val vs = pm.series.map { it.values }
                        RPointMatrix(
                                listOf(Metric.empty),
                                listOf(RPoints(timestamps, quantileFn(vs))),
                                ec.frames
                        )
                    } else {
                        val metAndGrouping = AggregatorPlanner.computeMetricsAndGrouping(pm.metrics, group)
                        RPointMatrix(
                                metAndGrouping.first,
                                metAndGrouping.second.map { selIdxes ->
                                    val vs = selIdxes.map { pm.series[it].values }
                                    RPoints(timestamps, quantileFn(vs))
                                },
                                ec.frames
                        )
                    }
                }
        )
        val builtins = (mappingAggregators + variadicAggregator).map { it.name to it }.toMap()
    }
}

class MappingAggregator(override val name: String, val fn: (List<DoubleSlice>) -> DoubleSlice) : Aggregator {
    fun eval(ec: ExecContext, metrics: List<Metric>, args: List<RuntimeValue>, grouping: List<IntArray>?): RuntimeValue {
        if (args.size != 1) {
            throw PromQLException("only 1 parameter expected for $name")
        }
        val pm = args[0] as? RPointMatrix ?: throw PromQLException("instant-vector expected for $name")
        if (pm.series.isEmpty()) {
            return RPointMatrix(pm.metrics, pm.series, ec.frames)
        }
        return if (grouping == null) {
            val vs = pm.series.map { it.values }
            // TODO: use GridMat
            RPointMatrix(
                    metrics,
                    listOf(
                            RPoints(LongSlice.wrap(ec.frames.toLongArray()), fn(vs))
                    ),
                    ec.frames
            )
        } else {
            val groups = grouping.map { grpIndexes ->
                grpIndexes.map { pm.series[it].values }
            }
            // TODO: use GridMat
            RPointMatrix(
                    metrics,
                    groups.map {
                        RPoints(LongSlice.wrap(ec.frames.toLongArray()), fn(it))
                    },
                    ec.frames
            )
        }
    }

}

class VariadicAggregator(override val name: String, val fn: (ec: ExecContext, args: List<RuntimeValue>, group: AggregatorGroup?) -> RuntimeValue) : Aggregator {
    fun eval(ec: ExecContext, args: List<RuntimeValue>, group: AggregatorGroup?): RuntimeValue {
        return fn(ec, args, group)
    }
}

