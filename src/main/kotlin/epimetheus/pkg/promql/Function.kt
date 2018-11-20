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
        val body: (List<Value>, EvalNode) -> Value) : Applicative {

    fun call(params: List<Value>, node: EvalNode): Value {
        if (!variadic && params.size != argTypes.size) {
            throw PromQLException("wrong number of arguments at function '$name' expected ${argTypes.size} but got ${params.size}")
        }
        try {
            val ret = this.body(params, node)
            return when (ret) {
                is GridMat -> ret.dropMetricName()
                else -> ret
            }
        } catch (iex: ClassCastException) {
            throw PromQLException("wrong type of arguments at function '$name' expected ${this.argTypes} but got ${params.map { it.javaClass }}")
        }
    }


    // TODO: UDF(User Defined Function) support?
    companion object {

        private fun extrapolatedRate(args: List<Value>, node: EvalNode, isCounter: Boolean, isRate: Boolean): GridMat {
            val m = args[0] as RangeGridMat
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

        private fun timeConvertUtil(calFlag: Int): (List<Value>, EvalNode) -> Value {
            return { args, node ->
                val cal = node.locale()
                if (args.isEmpty()) {
                    val ts = node.frames.toList()
                    cal.timeInMillis = System.currentTimeMillis()
                    val v = cal.get(calFlag).toDouble()
                    GridMat.of(ts, 0L, Metric.empty to DoubleArray(ts.size) { v })
                } else {
                    val m = args[0] as GridMat
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
                Function("abs") { args, node ->
                    simpleFn(args[0] as GridMat, node, Math::abs)
                },
                Function("absent") { _, _ -> TODO() },
                Function("avg_over_time", listOf(ValueType.Matrix)) { args, _ ->
                    val m = args[0] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        vs.sum() / vs.size
                    }
                },
                Function("ceil") { args, node ->
                    simpleFn(args[0] as GridMat, node, Math::ceil)
                },
                Function("changes", listOf(ValueType.Matrix)) { args, _ ->
                    val m = args[0] as RangeGridMat
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
                Function("clamp_max") { args, node ->
                    val m = args[0] as GridMat
                    val mx = args[1] as Scalar
                    simpleFn(m, node) { Math.min(it, mx.value) }
                },
                Function("clamp_min") { args, node ->
                    val m = args[0] as GridMat
                    val mx = args[1] as Scalar
                    simpleFn(m, node) { Math.max(it, mx.value) }
                },
                Function("count_over_time", listOf(ValueType.Matrix)) { args, node ->
                    val m = args[0] as RangeGridMat
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
                Function("days_in_month", variadic = true) { args, node ->
                    val cal = node.locale()
                    if (args.isEmpty()) {
                        val ts = node.frames.toList()
                        cal.timeInMillis = System.currentTimeMillis()
                        val ym = YearMonth.of(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH))
                        val v = ym.lengthOfMonth().toDouble()
                        GridMat.of(ts, 0L, Metric.empty to DoubleArray(ts.size) { v })
                    } else {
                        val m = args[0] as GridMat
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
                Function("delta", listOf(ValueType.Matrix)) { args, node ->
                    extrapolatedRate(args, node, false, false)
                },
                Function("deriv", listOf(ValueType.Matrix)) { _, _ -> TODO() },
                Function("exp") { args, node ->
                    simpleFn(args[0] as GridMat, node, Math::exp)
                },
                Function("floor") { args, node ->
                    simpleFn(args[0] as GridMat, node, Math::floor)
                },
                Function("histogram_quantile", listOf(ValueType.Scalar, ValueType.Vector)) { _, _ -> TODO() },
                Function("holt_winters", listOf(ValueType.Matrix, ValueType.Scalar, ValueType.Scalar)) { _, _ -> TODO() },
                Function("hour", variadic = true, body = timeConvertUtil(Calendar.HOUR_OF_DAY)),
                Function("idelta", listOf(ValueType.Matrix)) { args, node ->
                    instantValue(args, node, false)
                },
                Function("increase", listOf(ValueType.Matrix)) { args, node ->
                    extrapolatedRate(args, node, true, false)
                },
                Function("irate", listOf(ValueType.Matrix)) { args, node ->
                    instantValue(args, node, true)
                },
                Function("label_replace", listOf(ValueType.Vector, ValueType.String, ValueType.String, ValueType.String, ValueType.String)) { _, _ -> TODO() },
                Function("label_join", listOf(ValueType.Vector, ValueType.String, ValueType.String, ValueType.String), variadic = true) { _, _ -> TODO() },
                Function("ln") { args, node ->
                    simpleFn(args[0] as GridMat, node, Math::log)
                },
                Function("log10") { args, node ->
                    simpleFn(args[0] as GridMat, node, Math::log10)
                },
                Function("log2") { _, _ -> TODO() },
                Function("max_over_time", listOf(ValueType.Matrix)) { _, _ -> TODO() },
                Function("min_over_time", listOf(ValueType.Matrix)) { _, _ -> TODO() },
                Function("minute", variadic = true, body = timeConvertUtil(Calendar.MINUTE)),
                Function("month", variadic = true, body = timeConvertUtil(Calendar.MONTH)),
                Function("predict_linear", listOf(ValueType.Matrix, ValueType.Scalar)) { _, _ -> TODO() },
                Function("quantile_over_time", listOf(ValueType.Scalar, ValueType.Matrix)) { _, _ -> TODO() },
                Function("rate", listOf(ValueType.Matrix)) { args, node ->
                    extrapolatedRate(args, node, true, true)
                },
                Function("resets", listOf(ValueType.Matrix)) { args, _ ->
                    val m = args[0] as RangeGridMat
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
                Function("round") { args, node ->
                    simpleFn(args[0] as GridMat, node) { Math.round(it).toDouble() }
                },
                Function("scalar", returnType = ValueType.Scalar, variadic = true) { _, _ -> TODO() },
                Function("sort") { _, _ -> TODO() },
                Function("sort_desc") { _, _ -> TODO() },
                Function("sqrt") { args, _ ->
                    val m = args[0] as GridMat
                    m.mapRows { met, ts, vs ->
                        DoubleArray(vs.size) { Math.sqrt(vs[it]) }
                    }
                },
                Function("stddev_over_time", listOf(ValueType.Matrix)) { _, _ -> TODO() },
                Function("stdvar_over_time", listOf(ValueType.Matrix)) { _, _ -> TODO() },
                Function("sum_over_time", listOf(ValueType.Matrix)) { args, _ ->
                    val m = args[0] as RangeGridMat
                    m.applyUnifyFn { _, _, _, vs ->
                        vs.sum() / vs.size
                    }
                },
                Function("time", listOf(), returnType = ValueType.Scalar) { _, _ ->
                    Scalar(System.currentTimeMillis().toDouble() / 1000.0) // as seconds (UNIX Epoch)
                },
                Function("timestamp") { args, _ ->
                    val m = args[0] as GridMat
                    m.mapRows { met, ts, vs ->
                        DoubleArray(vs.size) { ts[it].toDouble() / 1000.0 } // ms --> s
                    }

                },
                Function("vector", listOf(ValueType.Scalar)) { args, node ->
                    val s = args[0] as Scalar
                    val ts = node.frames.toList()
                    GridMat.of(ts, 0L, Metric.empty to DoubleArray(ts.size) { s.value })
                },
                Function("year", variadic = true, body = timeConvertUtil(Calendar.YEAR))
        )
    }
}

