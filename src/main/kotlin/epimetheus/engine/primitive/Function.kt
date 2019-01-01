package epimetheus.engine.primitive

import epimetheus.EpimetheusException
import epimetheus.engine.ExecContext
import epimetheus.engine.graph.*
import epimetheus.engine.plan.*
import epimetheus.model.*
import epimetheus.pkg.promql.PromQLException
import epimetheus.pkg.promql.Utils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import java.time.Instant
import java.time.YearMonth
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoField
import java.util.regex.PatternSyntaxException

interface Function {
    val name: String
    fun plan(params: List<PlanNode>): PlanNode
    fun eval(ec: ExecContext, metrics: MetricPlan, args: List<RuntimeValue>, params: List<PlanNode>): RuntimeValue

    companion object {
        private fun extrapolatedRate(m: RRangeMatrix, frames: TimeFrames, isCounter: Boolean, isRate: Boolean): RPointMatrix {
            return m.unify { ts, timestamps, values ->
                val rangeStart = ts - m.range - m.offset
                val rangeEnd = ts - m.offset

                if (values.size < 2) {
                    return@unify Mat.StaleValue
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
                    resultValue /= (m.range.toDouble() / 1000.0)
                }
                resultValue
            }
        }

        private fun linearRegression(vs: DoubleSlice, ts: LongSlice, interceptTime: Long): DoubleArray {
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

        private fun instantValue(m: RRangeMatrix, frames: TimeFrames, isRate: Boolean): RPointMatrix {
            return m.unify { ts, timestamps, values ->
                if (values.size < 2) {
                    return@unify Mat.StaleValue
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
                    return@unify Mat.StaleValue
                }
                if (isRate) {
                    resultValue / (sampledInterval.toDouble() / 1000.0)
                } else {

                    resultValue
                }
            }
        }

        val mapFunctions = listOf(
                MapFunction("abs") { _, args ->
                    val m = args[0] as RPointMatrix
                    m.mapValues { d, _, _ -> Math.abs(d) }
                },
                MapFunction("avg_over_time") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    m.unify { _, _, vs ->
                        vs.average()
                    }
                },
                MapFunction("ceil") { _, args ->
                    val m = args[0] as RPointMatrix
                    m.mapValues { d, _, _ -> Math.ceil(d) }
                },
                MapFunction("changes") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    m.unify { _, _, vs ->
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
                    }
                },
                MapFunction("clamp_max") { _, args ->
                    val m = args[0] as RPointMatrix
                    val p = args[1] as RScalar
                    val v = p.value
                    m.mapValues { d, _, _ -> Math.min(d, v) }
                },
                MapFunction("clamp_min") { _, args ->
                    val m = args[0] as RPointMatrix
                    val p = args[1] as RScalar
                    val v = p.value
                    m.mapValues { d, _, _ -> Math.max(d, v) }
                },
                MapFunction("count_over_time") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    m.unify { _, _, vs ->
                        vs.count { !Mat.isStale(it) }.toDouble()
                    }
                },
                MapFunction("delta") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    extrapolatedRate(m, ctx.frames, false, false)
                },
                MapFunction("deriv") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    m.unify { ts, timestamps, values ->
                        val res = linearRegression(values, timestamps, timestamps[0])
                        res[0] // return slope
                    }
                },
                MapFunction("exp") { _, args ->
                    val m = args[0] as RPointMatrix
                    m.mapValues { d, _, _ -> Math.exp(d) }
                },
                MapFunction("floor") { _, args ->
                    val m = args[0] as RPointMatrix
                    m.mapValues { d, _, _ -> Math.floor(d) }
                },
                MapFunction("holt_winters") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    val sf = (args[1] as RScalar).value // smoothing factor
                    val tf = (args[2] as RScalar).value // trend factor
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
                    m.unify { ts, timestamps, values ->
                        val l = values.size
                        if (l < 2) {
                            return@unify Mat.StaleValue
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
                    }
                },
                MapFunction("idelta") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    instantValue(m, ctx.frames, false)
                },
                MapFunction("increase") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    extrapolatedRate(m, ctx.frames, true, false)
                },
                MapFunction("irate") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    instantValue(m, ctx.frames, true)
                },
                MapFunction("ln") { _, args ->
                    val m = args[0] as RPointMatrix
                    m.mapValues { d, _, _ -> Math.log(d) }
                },
                MapFunction("log10") { _, args ->
                    val m = args[0] as RPointMatrix
                    m.mapValues { d, _, _ -> Math.log10(d) }
                },
                MapFunction("log2") { _, args ->
                    val m = args[0] as RPointMatrix
                    val base = Math.log(2.0)
                    m.mapValues { d, _, _ -> Math.log(d) / base }
                },
                MapFunction("max_over_time") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    m.unify { _, _, vs ->
                        var ret: Double? = null
                        // Mat.StaleValue is a NaN but vs.max() returns NaN if there is any NaN.
                        for (v in vs) {
                            if (!Mat.isStale(v)) {
                                if (ret == null) {
                                    ret = v
                                } else if (v > ret) {
                                    ret = v
                                }
                            }
                        }
                        ret ?: Mat.StaleValue
                    }
                },
                MapFunction("min_over_time") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    m.unify { _, _, vs ->
                        var ret: Double? = null
                        // Mat.StaleValue is a NaN but vs.max() returns NaN if there is any NaN.
                        for (v in vs) {
                            if (!Mat.isStale(v)) {
                                if (ret == null) {
                                    ret = v
                                } else if (v < ret!!) { // why !! needs here?
                                    ret = v
                                }
                            }
                        }
                        ret ?: Mat.StaleValue
                    }
                },
                MapFunction("predict_linear") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    val s = args[1] as RScalar
                    m.unify { ts, timestamps, values ->
                        val res = linearRegression(values, timestamps, ts)
                        val slope = res[0]
                        val intercept = res[1]
                        slope * s.value + intercept
                    }
                },
                MapFunction("quantile_over_time", mainParamIndex = 1) { ctx, args ->
                    val q = (args[0] as RScalar).value
                    val m = args[1] as RRangeMatrix
                    m.unify { _, _, values ->
                        if (values.isEmpty()) {
                            return@unify Mat.StaleValue
                        }
                        val heap = values.clone()
                        Utils.quantile(q, heap.values)
                    }
                },
                MapFunction("rate") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    extrapolatedRate(m, ctx.frames, true, true)
                },
                MapFunction("resets") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    m.unify { _, _, vs ->
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
                    }
                },
                MapFunction("round") { _, args ->
                    val m = args[0] as RPointMatrix
                    val toNearest = if (args.size >= 2) {
                        val a1 = args[1]
                        when (a1) {
                            is RScalar -> a1.value
                            else -> throw PromQLException("scalar expected for round second argument, but got ${a1.javaClass}")
                        }
                    } else {
                        1.0
                    }
                    val toNearestInverse = 1.0 / toNearest
                    m.mapValues { d, _, _ -> Math.floor(d * toNearestInverse + 0.5) / toNearestInverse }
                },
                MapFunction("sqrt") { _, args ->
                    val m = args[0] as RPointMatrix
                    m.mapValues { d, _, _ -> Math.sqrt(d) }
                },
                MapFunction("stddev_over_time") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    m.unify { _, _, values ->
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
                    }
                },
                MapFunction("stdvar_over_time") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    m.unify { _, _, values ->
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
                    }
                },
                MapFunction("sum_over_time") { ctx, args ->
                    val m = args[0] as RRangeMatrix
                    m.unify { _, _, vs ->
                        vs.sum()
                    }
                },
                MapFunction("timestamp") { ctx, args ->
                    val m = args[0] as RPointMatrix
                    m.mapValues { _, ts, _ -> ts.toDouble() / 1000.0 }
                }
        )

        val chronoFunctions = listOf(
                ChronoFunction("days_of_month") { it[ChronoField.DAY_OF_MONTH] },
                ChronoFunction("days_of_week") { it[ChronoField.DAY_OF_WEEK] },
                ChronoFunction("day_of_month") { it[ChronoField.DAY_OF_MONTH] },
                ChronoFunction("day_of_week") { it[ChronoField.DAY_OF_WEEK] },
                ChronoFunction("hour") { it[ChronoField.HOUR_OF_DAY] },
                ChronoFunction("minute") { it[ChronoField.MINUTE_OF_HOUR] },
                ChronoFunction("month") { it[ChronoField.MONTH_OF_YEAR] },
                ChronoFunction("year") { it[ChronoField.YEAR] },
                ChronoFunction("days_in_month") { zdt ->
                    YearMonth.of(zdt[ChronoField.YEAR], zdt[ChronoField.MONTH_OF_YEAR]).lengthOfMonth()
                }

        )
        val otherFunctions = listOf(
                object : Function {
                    override val name = "absent"

                    override fun plan(params: List<PlanNode>): PlanNode {
                        // absent couldn't pre-compute metrics because it must handle staleness
                        return FunctionNode(name, VariableMetric, params)
                    }

                    override fun eval(ec: ExecContext, metrics: MetricPlan, args: List<RuntimeValue>, params: List<PlanNode>): RuntimeValue {
                        // absent function shoud try to be smart about its returing labels if passed argument is instant-selecot
                        val m = args[0] as RPointMatrix
                        if (m.series.any { !it.isEmpty() }) {
                            return RPointMatrix(listOf(), listOf(), ec.frames)
                        }
                        val p0 = params[0]
                        val met = when (p0) {
                            is MergePointNode -> {
                                if (p0.selectorHint == null) {
                                    Metric.empty
                                } else {
                                    val resSel = p0.selectorHint.matcher.matchers
                                            .filter { it.second.lmt == LabelMatchType.Eq && it.second.value != Metric.nameLabel }
                                            .map { arrayOf(it.first, it.second.value) }
                                    MetricBuilder(resSel.toMutableList()).build()
                                }
                            }
                            else -> Metric.empty
                        }
                        return RPointMatrix(listOf(met), listOf(RPoints.init(ec.frames) { _, _ -> 1.0 }), ec.frames)
                    }
                },
                object : Function {
                    override val name = "time"

                    override fun plan(params: List<PlanNode>): PlanNode {
                        return FunctionNode(name, VariableMetric, params)
                    }

                    override fun eval(ec: ExecContext, metrics: MetricPlan, args: List<RuntimeValue>, params: List<PlanNode>): RuntimeValue {
                        return RScalar(ec.frames.first() / 1000.0)
                    }
                },
                object : Function {
                    override val name = "vector"

                    override fun plan(params: List<PlanNode>): PlanNode {
                        return FunctionNode(name, VariableMetric, params)
                    }

                    override fun eval(ec: ExecContext, metrics: MetricPlan, args: List<RuntimeValue>, params: List<PlanNode>): RuntimeValue {
                        val arg = args[0] as RScalar
                        return RPointMatrix(listOf(Metric.empty), listOf(RPoints.init(ec.frames) { _, _ -> arg.value }), ec.frames)
                    }
                },
                object : Function {
                    override val name = "scalar"

                    override fun plan(params: List<PlanNode>): PlanNode {
                        return FunctionNode(name, VariableMetric, params)
                    }

                    override fun eval(ec: ExecContext, metrics: MetricPlan, args: List<RuntimeValue>, params: List<PlanNode>): RuntimeValue {
                        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
                    }
                },
                object : Function {
                    override val name = "histogram_quantile"

                    override fun plan(params: List<PlanNode>): PlanNode {
                        return FunctionNode(name, VariableMetric, params)
                    }

                    override fun eval(ec: ExecContext, metrics: MetricPlan, args: List<RuntimeValue>, params: List<PlanNode>): RuntimeValue {
                        val q = (args[0] as RScalar).value
                        val m = args[1] as RPointMatrix

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
                        // TODO: check series size

                        val timestampSize = m.series[0].timestamps.size
                        val columAccumlator = Array<List<Pair<Metric, Double>>>(timestampSize) { emptyList() }

                        for (tsIdx in 0 until timestampSize) {
                            val sigToMetBuckets = Long2ObjectOpenHashMap<MetricWithBuckets>()
                            for (metIdx in 0 until m.metrics.size) {
                                val met = m.metrics[metIdx]
                                val upperBound = Utils.parseDouble(met.get(Metric.bucketLabel))
                                        ?: continue // TODO: warn
                                val hash = sigf(met)
                                val mb = sigToMetBuckets[hash]
                                if (mb == null) {
                                    val newMet = met.filterWithout(true, listOf(Metric.bucketLabel))
                                    sigToMetBuckets[hash] = MetricWithBuckets(newMet, mutableListOf(Bucket(upperBound, m.series[metIdx].values[tsIdx])))
                                } else {
                                    mb.buckets.add(Bucket(upperBound, m.series[metIdx].values[tsIdx]))
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
                        return RPointMatrix(
                                metrics,
                                metrics.map { met ->
                                    RPoints.init(ec.frames) { tsIdx, _ ->
                                        columAccumlator[tsIdx].firstOrNull {
                                            it.first == met
                                        }?.second ?: Mat.StaleValue
                                    }
                                },
                                ec.frames)
                    }
                },
                object : Function {
                    override val name = "label_replace"

                    override fun plan(params: List<PlanNode>): PlanNode {
                        return FunctionNode(name, VariableMetric, params)
                    }

                    override fun eval(ec: ExecContext, metrics: MetricPlan, args: List<RuntimeValue>, params: List<PlanNode>): RuntimeValue {
                        val m = args[0] as RPointMatrix
                        val dst = args[1] as RString
                        val repl = args[2] as RString
                        val src = args[3] as RString
                        val regexStr = args[4] as RString
                        try {
                            val replacePat = Regex("""\$(\d+)""")
                            val pat = Regex(regexStr.value)
                            val dedupeCache = mutableSetOf<Signature>()
                            val replacedMetrics = m.metrics.map {
                                val srcVal = it.get(src.value) ?: ""
                                val match = pat.matchEntire(srcVal)
                                if (match == null) {
                                    val fp = it.fingerprint()
                                    if (dedupeCache.contains(fp)) {
                                        throw PromQLException("label_replace cannot create duplicated metrics")
                                    }
                                    dedupeCache.add(fp)
                                    return@map it
                                } else {
                                    val dstVal = replacePat.replace(repl.value) { mr ->
                                        val g0 = mr.groups[1]!!
                                        val replIndex = g0.value.toInt()
                                        match.groups[replIndex]?.value ?: ""
                                    }
                                    if (!Utils.isValidLabelName(dst.value)) {
                                        throw PromQLException("${dst.value} is invalid as a label")
                                    }
                                    val mb = it.builder()
                                    if (dstVal.isEmpty()) {
                                        mb.remove(dst.value)
                                    } else {
                                        mb.put(dst.value, dstVal)
                                    }
                                    val met = mb.build()
                                    val fp = met.fingerprint()
                                    if (dedupeCache.contains(fp)) {
                                        throw PromQLException("label_replace cannot create duplicated metrics")
                                    }
                                    dedupeCache.add(met.fingerprint())
                                    return@map met
                                }
                            }
                            try {
                                return RPointMatrix(replacedMetrics, m.series, m.frames).sortSeries()
                            } catch (e: EpimetheusException) {
                                throw PromQLException(e.message)
                            }
                        } catch (pex: PatternSyntaxException) {
                            throw PromQLException("invalid regex at label_replace: ${pex.message}")
                        }
                    }
                },
                object : Function {
                    override val name = "label_join"

                    override fun plan(params: List<PlanNode>): PlanNode {
                        return FunctionNode(name, VariableMetric, params)
                    }

                    override fun eval(ec: ExecContext, metrics: MetricPlan, args: List<RuntimeValue>, params: List<PlanNode>): RuntimeValue {
                        val m = args[0] as RPointMatrix
                        val dst = args[1] as RString
                        val sep = args[2] as RString
                        val sources = args.drop(3).map {
                            it as RString
                            if (!Utils.isValidLabelName(it.value)) {
                                throw PromQLException("label name '${it.value}' at label_join is invalid")
                            }
                            it.value
                        }
                        try {
                            val replacedMetrics = m.metrics.map { met ->
                                val dstVal = sources.joinToString(sep.value) { srcMet -> met.get(srcMet) ?: "" }
                                val mb = met.builder()
                                if (!Utils.isValidLabelName(dst.value)) {
                                    throw PromQLException("${dst.value} is invalid as a label")
                                }
                                if (dstVal.isEmpty()) {
                                    mb.remove(dst.value)
                                } else {
                                    mb.put(dst.value, dstVal)
                                }
                                return@map mb.build()
                            }
                            try {
                                return RPointMatrix(replacedMetrics, m.series, m.frames).sortSeries()
                            } catch (e: EpimetheusException) {
                                throw PromQLException(e.message)
                            }
                        } catch (pex: PatternSyntaxException) {
                            throw PromQLException("invalid regex at label_replace: ${pex.message}")
                        }
                    }
                }
        )

        // sort, sort_desc

        val builtins = (mapFunctions + chronoFunctions + otherFunctions).map { it.name to it }.toMap()
    }
}

// needs radical improvements for mode generic implementation
abstract class FunctionBase(val mainParamIndex: Int = 0, val dropMetricName: Boolean = true) : Function {
    override fun plan(params: List<PlanNode>): PlanNode {
        if (params.size <= mainParamIndex) {
            throw PromQLException("function $name requires ${mainParamIndex + 1} arguments but got ${params.size}")
        }
        val pln = params[mainParamIndex]
        return when (pln) {
            is MergeNode -> {
                if (dropMetricName) {
                    MergePointNode(
                            pln.nodes.map {
                                val splittedParams = params.mapIndexed { i, p -> if (i == mainParamIndex) it else p}
                                val nameDroppedPlan = FixedMetric(it.metPlan.metrics.map { m ->
                                    m.filterWithout(true, listOf())
                                })
                                FixedFunctionNode(name, nameDroppedPlan, splittedParams, it.affinity)
                            }
                    )
                } else {
                    MergePointNode(
                            pln.nodes.map {
                                val splittedParams = params.mapIndexed { i, p -> if (i == mainParamIndex) it else p}
                                FixedFunctionNode(name, it.metPlan, splittedParams, it.affinity)
                            }
                    )
                }
            }
            else -> FunctionNode(name, VariableMetric, params)
        }
    }

    protected fun determineMetrics(metricPlan: MetricPlan, args: List<RuntimeValue>): List<Metric> {
        if (args.size <= mainParamIndex) {
            throw PromQLException("function $name requires ${mainParamIndex + 1} arguments but got ${args.size}")
        }
        return when (metricPlan) {
            is FixedMetric -> metricPlan.metrics
            else -> {
                val pln = args[mainParamIndex] as RPointMatrix // TODO: add RangeMatrix pattern
                val mp = pln.metrics
                return if (dropMetricName) {
                    mp.map { it.filterWithout(true, listOf()) }
                } else {
                    mp
                }
            }
        }
    }
}

class MapFunction(override val name: String, mainParamIndex: Int = 0, dropMetricName: Boolean = true, val fn: (ExecContext, List<RuntimeValue>) -> RPointMatrix) : FunctionBase(mainParamIndex, dropMetricName) {
    override fun eval(ec: ExecContext, metrics: MetricPlan, args: List<RuntimeValue>, params: List<PlanNode>): RuntimeValue {
        val mets = determineMetrics(metrics, args)
        val series = fn(ec, args).series
        assert(mets.size == series.size)
        return RPointMatrix(mets, series, ec.frames)
    }
}

open class ChronoFunction(override val name: String, val extractFn: (ZonedDateTime) -> Int) : FunctionBase() {
    override fun plan(params: List<PlanNode>): PlanNode {
        return if (params.isEmpty()) {
            FunctionNode(name, FixedMetric(listOf(Metric.empty)), listOf())
        } else {
            val p = params[0] as? InstantNode
                    ?: throw PromQLException("$name expects instant-vector but got ${params[0].javaClass}")
            val mp = p.metPlan
            when (mp) {
                is FixedMetric -> FunctionNode(name, FixedMetric(mp.metrics.map { it.filterWithout(true, listOf()) }), params)
                else -> FunctionNode(name, mp, params)
            }
        }
    }

    override fun eval(ec: ExecContext, metrics: MetricPlan, args: List<RuntimeValue>, params: List<PlanNode>): RuntimeValue {
        return if (args.isEmpty()) {
            val ts = ec.frames.toList()
            val zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(0), ZoneId.of("UTC"))
            val v = extractFn(zdt).toDouble()
            // TODO: use GridMat
            RPointMatrix(listOf(Metric.empty), listOf(RPoints(LongSlice.wrap(ts.toLongArray()), DoubleSlice.init(ts.size) { v })), ec.frames)
        } else {
            val m = args[0] as RPointMatrix
            val mets = determineMetrics(VariableMetric, args)
            val mapped = m.mapRow { vs, _, _ ->
                DoubleSlice.init(vs.size) {
                    val t = vs[it]
                    val zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(t.toLong()), ZoneId.of("UTC"))
                    extractFn(zdt).toDouble()
                }
            }
            RPointMatrix(mets, mapped.series, ec.frames)
        }
    }
}
