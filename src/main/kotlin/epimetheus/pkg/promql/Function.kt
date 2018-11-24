package epimetheus.pkg.promql

import epimetheus.EpimetheusException
import epimetheus.engine.EvalNode
import epimetheus.model.*
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import java.time.Instant
import java.time.YearMonth
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoField
import java.time.temporal.TemporalField
import java.util.*
import java.util.regex.PatternSyntaxException


data class Function(
        val name: String,
        override val argTypes: List<ValueType> = listOf(ValueType.Vector),
        override val returnType: ValueType = ValueType.Vector,
        override val variadic: Boolean = false,
        val isDropsMetricName: Boolean = false,
        val body: (List<Value>, List<Expression>, EvalNode) -> Value) : Applicative {

    fun call(vals: List<Value>, args: List<Expression>, node: EvalNode): Value {
        if (!variadic && vals.size != argTypes.size) {
            throw PromQLException("wrong number of arguments at function '$name' expected ${argTypes.size} but got ${vals.size}")
        }
        try {
            return this.body(vals, args, node)
        } catch (iex: ClassCastException) {
            throw PromQLException("wrong type of arguments at function '$name' expected ${this.argTypes} but got ${vals.map { it.javaClass }} $iex")
        }
    }


    // TODO: UDF(User Defined Function) support?
    companion object {

        private fun extrapolatedRate(vals: List<Value>, args: List<Expression>, node: EvalNode, isCounter: Boolean, isRate: Boolean): GridMat {
            val m = vals[0] as RangeGridMat
            return m.applyUnifyFn { _, ts, timestamps, values ->
                val rangeStart = ts - m.windowSize - m.offset
                val rangeEnd = ts - m.offset

                if (values.size < 2) {
                    return@applyUnifyFn Mat.StaleValue
                }

                var counterCorrection = 0.0
                for (j in 1 until values.size) {
                    if (isCounter && values[j] < values[j - 1]) {
                        counterCorrection += values[j - 1]
                    }
                }
                var resultValue = values.last() - values.first() + counterCorrection

                // Duration between first/last samples and boundary of range.
                var durationToStart = (timestamps.first() - rangeStart).toDouble() / 1000.0
                val durationToEnd = (rangeEnd - timestamps.last()).toDouble() / 1000.0

                val sampledInterval = (timestamps.last() - timestamps.first()).toDouble() / 1000.0
                val averageDurationBetweenSamples = sampledInterval / (timestamps.size - 1).toDouble()
                if (isCounter && resultValue > 0 && values[0] >= 0) {
                    val durationToZero = sampledInterval * (values.first() / resultValue)
                    if (durationToZero < durationToStart) {
                        durationToStart = durationToZero
                    }
                }
                val extrapolationThreshould = averageDurationBetweenSamples * 1.1
                var extrapolateToInterval = sampledInterval
                extrapolateToInterval += if (durationToStart < extrapolationThreshould) {
                    durationToStart
                } else {
                    averageDurationBetweenSamples / 2
                }
                extrapolateToInterval += if (durationToEnd < extrapolationThreshould) {
                    durationToEnd
                } else {
                    averageDurationBetweenSamples / 2
                }
                resultValue *= (extrapolateToInterval / sampledInterval)
                if (isRate) {
                    resultValue /= (m.windowSize.toDouble() / 1000.0)
                }
                resultValue
            }.dropMetricName()
        }

        private fun linearRegression(vs: DoubleArray, ts: LongArray, interceptTime: Long): DoubleArray {
            var n = 0.0
            var sumX = 0.0
            var sumY = 0.0
            var sumXY = 0.0
            var sumX2 = 0.0
            for (i in 0 until vs.size) {
                val x = (ts[i] - interceptTime) / 1000.0
                n += 1.0
                sumY += vs[i]
                sumX += x
                sumXY += x * vs[i]
                sumX2 += x * x
            }
            val covXY = sumXY - sumX * sumY / n
            val varX = sumX2 - sumX * sumX / n

            val slope = covXY / varX
            val intercept = sumY / n - slope * sumX / n

            return doubleArrayOf(slope, intercept)
        }

        private fun instantValue(args: List<Value>, node: EvalNode, isRate: Boolean): GridMat {
            val m = args[0] as RangeGridMat
            return m.applyUnifyFn { _, ts, timestamps, values ->
                if (values.size < 2) {
                    return@applyUnifyFn Mat.StaleValue
                }
                val iLast = values.size - 1
                val iPrev = values.size - 2
                val resultValue = if (isRate && values[iLast] < values[iPrev]) {
                    values[iLast]
                } else {
                    values[iLast] - values[iPrev]
                }
                val sampledInterval = timestamps[iLast] - timestamps[iPrev]
                if (sampledInterval == 0L) {
                    return@applyUnifyFn Mat.StaleValue
                }
                if (isRate) {
                    resultValue / (sampledInterval.toDouble() / 1000.0)
                } else {

                    resultValue
                }
            }.dropMetricName()
        }

        private fun timeConvertUtil(field: TemporalField): (List<Value>, List<Expression>, EvalNode) -> Value {
            return { vals, _, node ->
                if (vals.isEmpty()) {
                    val ts = node.frames.toList()
                    val zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(0), ZoneId.of("UTC"))
                    val v = zdt[field].toDouble()
                    GridMat.of(ts, 0L, Metric.empty to DoubleArray(ts.size) { v })
                } else {
                    val m = vals[0] as GridMat
                    m.mapRows { met, ts, vs ->
                        DoubleArray(vs.size) {
                            val t = vs[it]
                            val zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(t.toLong()), ZoneId.of("UTC"))
                            zdt[field].toDouble()
                        }
                    }.dropMetricName()
                }
            }
        }

        private fun simpleFn(m: GridMat, node: EvalNode, fn: (Double) -> Double): Value {
            return m.mapRows { met, ts, vs ->
                DoubleArray(vs.size) { fn(vs[it]) }
            }.dropMetricName()
        }

        val builtins = listOf(
                Function("abs") { vals, _, node ->
                    simpleFn(vals[0] as GridMat, node, Math::abs)
                },
                Function("absent") { vals, args, node ->
                    val m = vals[0] as GridMat
                    if (m.metrics.isNotEmpty()) {
                        return@Function GridMat(arrayOf(), node.frames, listOf())
                    }
                    val a0 = args[0]
                    val newMet = if (a0 is InstantSelector) {
                        val resSel = a0.matcher.matchers
                                .filter { it.second.lmt == LabelMatchType.Eq && it.second.value != Metric.nameLabel }
                                .map { it.first to it.second.value }
                                .toMap()
                                .toSortedMap()
                        Metric(resSel)
                    } else {
                        Metric.empty
                    }
                    GridMat(arrayOf(newMet), node.frames, listOf(DoubleArray(node.frames.size) { 1.0 }))

                },
                Function("avg_over_time", listOf(ValueType.Matrix)) { vals, _, _ ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        vs.sum() / vs.size
                    }.dropMetricName()
                },
                Function("ceil") { vals, _, node ->
                    simpleFn(vals[0] as GridMat, node, Math::ceil)
                },
                Function("changes", listOf(ValueType.Matrix)) { vals, _, _ ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        when {
                            vs.size < 2 -> 0.0
                            else -> {
                                var ctr = 0
                                for (i in 1 until vs.size) {
                                    if (vs[i] != vs[i - 1] && !(vs[i].isNaN() && vs[i - 1].isNaN())) {
                                        ctr += 1
                                    }
                                }
                                ctr.toDouble()
                            }
                        }
                    }.dropMetricName()
                },
                Function("clamp_max", listOf(ValueType.Vector, ValueType.Scalar)) { vals, _, node ->
                    val m = vals[0] as GridMat
                    val mx = vals[1] as Scalar
                    simpleFn(m, node) { Math.min(it, mx.value) }
                },
                Function("clamp_min", listOf(ValueType.Vector, ValueType.Scalar)) { vals, _, node ->
                    val m = vals[0] as GridMat
                    val mx = vals[1] as Scalar
                    simpleFn(m, node) { Math.max(it, mx.value) }
                },
                Function("count_over_time", listOf(ValueType.Matrix)) { vals, _, node ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        var ctr = 0
                        for (v in vs) {
                            if (!Mat.isStale(v)) {
                                ctr++
                            }
                        }
                        ctr.toDouble()
                    }.dropMetricName()
                },
                Function("days_in_month", variadic = true) { vals, _, node ->
                    if (vals.isEmpty()) {
                        val ts = node.frames.toList()
                        val zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(0), ZoneId.of("UTC"))
                        val ym = YearMonth.of(zdt[ChronoField.YEAR], zdt[ChronoField.MONTH_OF_YEAR])
                        GridMat.of(ts, 0L, Metric.empty to DoubleArray(ts.size) { ym.lengthOfMonth().toDouble() })
                    } else {
                        val m = vals[0] as GridMat
                        m.mapRows { met, ts, vs ->
                            DoubleArray(vs.size) {
                                val t = vs[it]
                                val zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(t.toLong()), ZoneId.of("UTC"))
                                val ym = YearMonth.of(zdt[ChronoField.YEAR], zdt[ChronoField.MONTH_OF_YEAR])
                                ym.lengthOfMonth().toDouble()
                            }
                        }.dropMetricName()
                    }
                },
                Function("days_of_month", variadic = true, body = timeConvertUtil(ChronoField.DAY_OF_MONTH)),
                Function("days_of_week", variadic = true, body = timeConvertUtil(ChronoField.DAY_OF_WEEK)),
                Function("day_of_month", variadic = true, body = timeConvertUtil(ChronoField.DAY_OF_MONTH)),
                Function("day_of_week", variadic = true, body = timeConvertUtil(ChronoField.DAY_OF_WEEK)),
                Function("delta", listOf(ValueType.Matrix)) { vals, args, node ->
                    extrapolatedRate(vals, args, node, false, false)
                },
                Function("deriv", listOf(ValueType.Matrix)) { vals, _, _ ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { m, ts, timestamps, values ->
                        val res = linearRegression(values, timestamps, timestamps[0])
                        res[0] // slope
                    }.dropMetricName()
                },
                Function("exp") { vals, _, node ->
                    simpleFn(vals[0] as GridMat, node, Math::exp)
                },
                Function("floor") { vals, _, node ->
                    simpleFn(vals[0] as GridMat, node, Math::floor)
                },
                Function("histogram_quantile", listOf(ValueType.Scalar, ValueType.Vector)) { vals, _, _ ->
                    val q = (vals[0] as Scalar).value
                    val m = vals[1] as GridMat

                    data class Bucket(val upperBound: Double, val count: Double)
                    data class MetricWithBuckets(val metric: Metric, val buckets: MutableList<Bucket>)

                    fun bucketQuantile(buckets: MutableList<Bucket>): Double {
                        if (q < 0) {
                            return Double.NEGATIVE_INFINITY
                        }
                        if (q > 1) {
                            return Double.POSITIVE_INFINITY
                        }
                        if (buckets.size < 2) {
                            return Double.NaN
                        }
                        buckets.sortBy { it.upperBound }
                        fun ensureMonotonic(buckets: MutableList<Bucket>) {
                            var max = buckets.first().count
                            for (i in 1 until buckets.size) {
                                when {
                                    buckets[i].count > max -> max = buckets[i].count
                                    buckets[i].count < max -> buckets[i] = Bucket(buckets[i].upperBound, max)
                                }
                            }
                        }
                        ensureMonotonic(buckets)
                        var rank = q * buckets.last().count
                        val b = buckets.indexOfFirst { it.count >= rank } // TODO: binary search
                        if (b == buckets.size - 1) {
                            return buckets[buckets.size - 2].upperBound
                        }
                        if (b == 0 && buckets.first().upperBound <= 0) {
                            return buckets.first().upperBound
                        }
                        var bucketStart = 0.0
                        val bucketEnd = buckets[b].upperBound
                        var count = buckets[b].count
                        if (b > 0) {
                            bucketStart = buckets[b - 1].upperBound
                            count -= buckets[b - 1].count
                            rank -= buckets[b - 1].count
                        }
                        return bucketStart + (bucketEnd - bucketStart) * (rank / count)
                    }

                    fun sigf(met: Metric): Long {
                        return met.filteredFingerprint(false, listOf(Metric.bucketLabel), true)
                    }

                    val columAccumlator = Array<List<Pair<Metric, Double>>>(m.timestamps.size) { emptyList() }

                    for (tsIdx in 0 until m.timestamps.size) {
                        val sigToMetBuckets = Long2ObjectOpenHashMap<MetricWithBuckets>()
                        for (metIdx in 0 until m.metrics.size) {
                            val met = m.metrics[metIdx]
                            val upperBound = Utils.parseDouble(met.m[Metric.bucketLabel]) ?: continue // TODO: warn
                            val hash = sigf(met)
                            val mb = sigToMetBuckets[hash]
                            if (mb == null) {
                                val newMet = met.filterWithout(true, listOf(Metric.bucketLabel))
                                sigToMetBuckets[hash] = MetricWithBuckets(newMet, mutableListOf(Bucket(upperBound, m.values[metIdx][tsIdx])))
                            } else {
                                mb.buckets.add(Bucket(upperBound, m.values[metIdx][tsIdx]))
                            }
                        }
                        val res = mutableListOf<Pair<Metric, Double>>()
                        sigToMetBuckets.values.forEach { mb ->
                            if (!mb.buckets.isEmpty()) {
                                res.add(mb.metric to bucketQuantile(mb.buckets))
                            }
                        }
                        columAccumlator[tsIdx] = res
                    }
                    val metrics = columAccumlator
                            .flatMap { col -> col.map { kv -> kv.first } }
                            .distinct()
                            .sortedBy { it.fingerprint() }
                    GridMat(metrics.toTypedArray(), m.timestamps, metrics.map { met ->
                        DoubleArray(m.timestamps.size) { tsIdx ->
                            columAccumlator[tsIdx].firstOrNull { it.first == met }?.second ?: Mat.StaleValue
                        }
                    })
                },
                Function("holt_winters", listOf(ValueType.Matrix, ValueType.Scalar, ValueType.Scalar)) { vals, _, _ ->
                    val m = vals[0] as RangeGridMat
                    val sf = (vals[1] as Scalar).value // smoothing factor
                    val tf = (vals[2] as Scalar).value // trend factor
                    fun calcTrendValues(i: Int, s0: Double, s1: Double, b: Double): Double {
                        if (i == 0) {
                            return b
                        }
                        val x = tf * (s1 - s0)
                        val y = (1 - tf) * b
                        return x + y
                    }
                    if (sf <= 0.0 || sf >= 1.0) {
                        throw PromQLException("invalid smoothing factor. Expected: 0 < sf < 1, got: ${sf}")
                    }
                    if (tf <= 0.0 || tf >= 1.0) {
                        throw PromQLException("invalid trend factor. Expected: 0 < tf < 1, got: ${tf}")
                    }
                    m.applyUnifyFn { metric, ts, timestamps, values ->
                        val l = values.size
                        if (l < 2) {
                            return@applyUnifyFn Mat.StaleValue
                        }
                        var s0 = 0.0
                        var s1 = values.first()
                        var b = values[1] - values[0]
                        var x = 0.0
                        var y = 0.0
                        for (i in 1 until l) {
                            x = sf * values[i]
                            b = calcTrendValues(i - 1, s0, s1, b)
                            y = (1 - sf) * (s1 + b)
                            s0 = s1
                            s1 = x + y
                        }
                        s1
                    }.dropMetricName()
                },
                Function("hour", variadic = true, body = timeConvertUtil(ChronoField.HOUR_OF_DAY)),
                Function("idelta", listOf(ValueType.Matrix)) { vals, _, node ->
                    instantValue(vals, node, false)
                },
                Function("increase", listOf(ValueType.Matrix)) { vals, args, node ->
                    extrapolatedRate(vals, args, node, true, false)
                },
                Function("irate", listOf(ValueType.Matrix)) { vals, _, node ->
                    instantValue(vals, node, true)
                },
                Function("label_replace", listOf(ValueType.Vector, ValueType.String, ValueType.String, ValueType.String, ValueType.String)) { vals, _, _ ->
                    val m = vals[0] as GridMat
                    val dst = vals[1] as StringValue
                    val repl = vals[2] as StringValue
                    val src = vals[3] as StringValue
                    val regexStr = vals[4] as StringValue
                    try {
                        val replacePat = Regex("""\$(\d+)""")
                        val pat = Regex(regexStr.value)
                        val replacedMetrics = m.metrics.map {
                            val srcVal = it.m[src.value] ?: ""
                            val match = pat.matchEntire(srcVal)
                            if (match == null) {
                                return@map it
                            } else {
                                val dstVal = replacePat.replace(repl.value) { mr ->
                                    val g0 = mr.groups[1]!!
                                    val replIndex = g0.value.toInt()
                                    match.groups[replIndex]?.value ?: ""
                                }
                                val newMet = it.m.toSortedMap() // copy needs here
                                if (!Utils.isValidLabelName(dst.value)) {
                                    throw PromQLException("${dst.value} is invalid as a label")
                                }
                                if (dstVal.isEmpty()) {
                                    newMet.remove(dst.value)
                                } else {
                                    newMet[dst.value] = dstVal
                                }
                                return@map Metric(newMet)
                            }
                        }
                        try {
                            GridMat.withSortting(replacedMetrics, m.timestamps, m.values)
                        } catch (e: EpimetheusException) {
                            throw PromQLException(e.message)
                        }
                    } catch (pex: PatternSyntaxException) {
                        throw PromQLException("invalid regex at label_replace: ${pex.message}")
                    }
                },
                Function("label_join", listOf(ValueType.Vector, ValueType.String, ValueType.String, ValueType.String), variadic = true) { vals, _, _ ->
                    val m = vals[0] as GridMat
                    val dst = vals[1] as StringValue
                    val sep = vals[2] as StringValue
                    val sources = vals.drop(3).map {
                        it as StringValue
                        if (!Utils.isValidLabelName(it.value)) {
                            throw PromQLException("label name '${it.value}' at label_join is invalid")
                        }
                        it.value
                    }
                    try {
                        val replacedMetrics = m.metrics.map { met ->
                            val dstVal = sources.joinToString(sep.value) { srcMet -> met.m[srcMet] ?: "" }
                            val newMet = met.m.toSortedMap() // copy needs here
                            if (dstVal.isEmpty()) {
                                newMet.remove(dst.value)
                            } else {
                                newMet[dst.value] = dstVal
                            }
                            if (!Utils.isValidLabelName(dst.value)) {
                                throw PromQLException("${dst.value} is invalid as a label")
                            }
                            return@map Metric(newMet)
                        }
                        try {
                            GridMat.withSortting(replacedMetrics, m.timestamps, m.values)
                        } catch (e: EpimetheusException) {
                            throw PromQLException(e.message)
                        }
                    } catch (pex: PatternSyntaxException) {
                        throw PromQLException("invalid regex at label_replace: ${pex.message}")
                    }
                },
                Function("ln") { vals, _, node ->
                    simpleFn(vals[0] as GridMat, node, Math::log)
                },
                Function("log10") { vals, _, node ->
                    simpleFn(vals[0] as GridMat, node, Math::log10)
                },
                Function("log2") { vals, _, node ->
                    simpleFn(vals[0] as GridMat, node) {
                        Math.log(it) / Math.log(2.0)
                    }
                },
                Function("max_over_time", listOf(ValueType.Matrix)) { vals, _, node ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        if (vs.isEmpty()) {
                            Mat.StaleValue
                        } else {
                            vs.max()!!
                        }
                    }.dropMetricName()
                },
                Function("min_over_time", listOf(ValueType.Matrix)) { vals, _, node ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        if (vs.isEmpty()) {
                            Mat.StaleValue
                        } else {
                            vs.min()!!
                        }
                    }.dropMetricName()
                },
                Function("minute", variadic = true, body = timeConvertUtil(ChronoField.MINUTE_OF_HOUR)),
                Function("month", variadic = true, body = timeConvertUtil(ChronoField.MONTH_OF_YEAR)),
                Function("predict_linear", listOf(ValueType.Matrix, ValueType.Scalar)) { vals, _, _ ->
                    val m = vals[0] as RangeGridMat
                    val s = vals[1] as Scalar
                    m.applyUnifyFn { m, ts, timestamps, values ->
                        val res = linearRegression(values, timestamps, ts)
                        val slope = res[0]
                        val intercept = res[1]
                        slope * s.value + intercept
                    }.dropMetricName()
                },
                Function("quantile_over_time", listOf(ValueType.Scalar, ValueType.Matrix)) { vals, _, _ ->
                    val q = (vals[0] as Scalar).value
                    val m = vals[1] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        if (vs.isEmpty()) {
                            return@applyUnifyFn Mat.StaleValue
                        }
                        val heap = Arrays.copyOf(vs, vs.size)
                        Utils.quantile(q, heap)
                    }.dropMetricName()
                },
                Function("rate", listOf(ValueType.Matrix)) { vals, args, node ->
                    extrapolatedRate(vals, args, node, true, true)
                },
                Function("resets", listOf(ValueType.Matrix)) { vals, _, _ ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        when {
                            vs.size < 2 -> 0.0
                            else -> {
                                var leapCtr = 0
                                for (i in 1 until vs.size) {
                                    if (vs[i] < vs[i - 1]) {
                                        leapCtr += 1
                                    }
                                }
                                leapCtr.toDouble()
                            }
                        }
                    }.dropMetricName()
                },
                Function("round", variadic = true) { vals, _, node ->
                    val m = vals[0] as GridMat
                    val toNearest = if (vals.size >= 2) {
                        val a1 = vals[1]
                        when (a1) {
                            is Scalar -> a1.value
                            else -> throw PromQLException("scalar expected for round second argument, but got ${a1.javaClass}")
                        }
                    } else {
                        1.0
                    }
                    val toNearestInverse = 1.0 / toNearest
                    simpleFn(m, node) {
                        Math.floor(it * toNearestInverse + 0.5) / toNearestInverse
                    }
                },
                Function("scalar", returnType = ValueType.Scalar, variadic = true) { vals, _, _ -> TODO() },
                Function("sort") { vals, _, _ -> TODO() },
                Function("sort_desc") { vals, _, _ -> TODO() },
                Function("sqrt") { vals, _, node ->
                    val m = vals[0] as GridMat
                    simpleFn(m, node, Math::sqrt)
                },
                Function("stddev_over_time", listOf(ValueType.Matrix)) { vals, _, _ ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { metric, ts, timestamps, values ->
                        var aux = 0.0
                        var count = 0.0
                        var mean = 0.0
                        for (i in 0 until values.size) {
                            count++
                            val delta = values[i] - mean
                            mean += delta / count
                            aux += delta * (values[i] - mean)
                        }
                        Math.sqrt(aux / count)
                    }.dropMetricName()
                },
                Function("stdvar_over_time", listOf(ValueType.Matrix)) { vals, _, _ ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { metric, ts, timestamps, values ->
                        var aux = 0.0
                        var count = 0.0
                        var mean = 0.0
                        for (i in 0 until values.size) {
                            count++
                            val delta = values[i] - mean
                            mean += delta / count
                            aux += delta * (values[i] - mean)
                        }
                        aux / count
                    }.dropMetricName()
                },
                Function("sum_over_time", listOf(ValueType.Matrix)) { vals, _, _ ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        vs.sum()
                    }.dropMetricName()
                },
                Function("time", listOf(), returnType = ValueType.Scalar) { vals, _, node ->
                    Scalar(node.frames.first().toDouble() / 1000.0)
                },
                Function("timestamp") { vals, _, _ ->
                    val m = vals[0] as GridMat
                    m.mapRows { met, ts, vs ->
                        DoubleArray(vs.size) { ts[it].toDouble() / 1000.0 } // ms --> s
                    }.dropMetricName()

                },
                Function("vector", listOf(ValueType.Scalar)) { vals, _, node ->
                    val s = vals[0] as Scalar
                    val ts = node.frames.toList()
                    GridMat.of(ts, 0L, Metric.empty to DoubleArray(ts.size) { s.value })
                },
                Function("year", variadic = true, body = timeConvertUtil(ChronoField.YEAR))
        )
    }
}

