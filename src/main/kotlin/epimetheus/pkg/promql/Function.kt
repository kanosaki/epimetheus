package epimetheus.pkg.promql

import epimetheus.engine.EvalNode
import epimetheus.model.*
import java.time.YearMonth
import java.util.*


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
            val ret = this.body(vals, args, node)
            return when (ret) {
                is GridMat -> ret.dropMetricName()
                else -> ret
            }
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
            }
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
            }
        }

        private fun timeConvertUtil(calFlag: Int): (List<Value>, List<Expression>, EvalNode) -> Value {
            return { vals, _, node ->
                val cal = node.locale()
                if (vals.isEmpty()) {
                    val ts = node.frames.toList()
                    cal.timeInMillis = System.currentTimeMillis()
                    val v = cal.get(calFlag).toDouble()
                    GridMat.of(ts, 0L, Metric.empty to DoubleArray(ts.size) { v })
                } else {
                    val m = vals[0] as GridMat
                    m.mapRows { met, ts, vs ->
                        DoubleArray(vs.size) {
                            val t = vs[it]
                            cal.timeInMillis = t.toLong()
                            cal.get(calFlag).toDouble()
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
                    }
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
                                    if (vs[i] != vs[i - 1]) {
                                        ctr += 1
                                    }
                                }
                                ctr.toDouble()
                            }
                        }
                    }
                },
                Function("clamp_max") { vals, _, node ->
                    val m = vals[0] as GridMat
                    val mx = vals[1] as Scalar
                    simpleFn(m, node) { Math.min(it, mx.value) }
                },
                Function("clamp_min") { vals, _, node ->
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
                    }
                },
                Function("days_in_month", variadic = true) { vals, _, node ->
                    val cal = node.locale()
                    if (vals.isEmpty()) {
                        val ts = node.frames.toList()
                        cal.timeInMillis = System.currentTimeMillis()
                        val ym = YearMonth.of(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH))
                        val v = ym.lengthOfMonth().toDouble()
                        GridMat.of(ts, 0L, Metric.empty to DoubleArray(ts.size) { v })
                    } else {
                        val m = vals[0] as GridMat
                        m.mapRows { met, ts, vs ->
                            DoubleArray(vs.size) {
                                val t = vs[it]
                                cal.timeInMillis = t.toLong()
                                val ym = YearMonth.of(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH))
                                ym.lengthOfMonth().toDouble()
                            }
                        }
                    }
                },
                Function("days_of_month", variadic = true, body = timeConvertUtil(Calendar.DAY_OF_MONTH)),
                Function("days_of_week", variadic = true, body = timeConvertUtil(Calendar.DAY_OF_WEEK)),
                Function("day_of_month", variadic = true, body = timeConvertUtil(Calendar.DAY_OF_MONTH)),
                Function("day_of_week", variadic = true, body = timeConvertUtil(Calendar.DAY_OF_WEEK)),
                Function("delta", listOf(ValueType.Matrix)) { vals, args, node ->
                    extrapolatedRate(vals, args, node, false, false)
                },
                Function("deriv", listOf(ValueType.Matrix)) { vals, _, _ -> TODO() },
                Function("exp") { vals, _, node ->
                    simpleFn(vals[0] as GridMat, node, Math::exp)
                },
                Function("floor") { vals, _, node ->
                    simpleFn(vals[0] as GridMat, node, Math::floor)
                },
                Function("histogram_quantile", listOf(ValueType.Scalar, ValueType.Vector)) { vals, _, _ -> TODO() },
                Function("holt_winters", listOf(ValueType.Matrix, ValueType.Scalar, ValueType.Scalar)) { vals, _, _ -> TODO() },
                Function("hour", variadic = true, body = timeConvertUtil(Calendar.HOUR_OF_DAY)),
                Function("idelta", listOf(ValueType.Matrix)) { vals, _, node ->
                    instantValue(vals, node, false)
                },
                Function("increase", listOf(ValueType.Matrix)) { vals, args, node ->
                    extrapolatedRate(vals, args, node, true, false)
                },
                Function("irate", listOf(ValueType.Matrix)) { vals, _, node ->
                    instantValue(vals, node, true)
                },
                Function("label_replace", listOf(ValueType.Vector, ValueType.String, ValueType.String, ValueType.String, ValueType.String)) { vals, _, _ -> TODO() },
                Function("label_join", listOf(ValueType.Vector, ValueType.String, ValueType.String, ValueType.String), variadic = true) { vals, _, _ -> TODO() },
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
                    }
                },
                Function("min_over_time", listOf(ValueType.Matrix)) { vals, _, node ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        if (vs.isEmpty()) {
                            Mat.StaleValue
                        } else {
                            vs.min()!!
                        }
                    }
                },
                Function("minute", variadic = true, body = timeConvertUtil(Calendar.MINUTE)),
                Function("month", variadic = true, body = timeConvertUtil(Calendar.MONTH)),
                Function("predict_linear", listOf(ValueType.Matrix, ValueType.Scalar)) { vals, _, _ -> TODO() },
                Function("quantile_over_time", listOf(ValueType.Scalar, ValueType.Matrix)) { vals, _, _ -> TODO() },
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
                    }
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
                Function("sqrt") { vals, _, _ ->
                    val m = vals[0] as GridMat
                    m.mapRows { met, ts, vs ->
                        DoubleArray(vs.size) { Math.sqrt(vs[it]) }
                    }
                },
                Function("stddev_over_time", listOf(ValueType.Matrix)) { vals, _, _ -> TODO() },
                Function("stdvar_over_time", listOf(ValueType.Matrix)) { vals, _, _ -> TODO() },
                Function("sum_over_time", listOf(ValueType.Matrix)) { vals, _, _ ->
                    val m = vals[0] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        vs.sum()
                    }
                },
                Function("time", listOf(), returnType = ValueType.Scalar) { vals, _, node ->
                    Scalar(node.frames.first().toDouble() / 1000.0)
                },
                Function("timestamp") { vals, _, _ ->
                    val m = vals[0] as GridMat
                    m.mapRows { met, ts, vs ->
                        DoubleArray(vs.size) { ts[it].toDouble() / 1000.0 } // ms --> s
                    }

                },
                Function("vector", listOf(ValueType.Scalar)) { vals, _, node ->
                    val s = vals[0] as Scalar
                    val ts = node.frames.toList()
                    GridMat.of(ts, 0L, Metric.empty to DoubleArray(ts.size) { s.value })
                },
                Function("year", variadic = true, body = timeConvertUtil(Calendar.YEAR))
        )
    }
}

