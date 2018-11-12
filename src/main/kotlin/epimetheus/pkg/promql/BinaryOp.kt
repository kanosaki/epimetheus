package epimetheus.pkg.promql

import epimetheus.model.*

data class BinaryOp(
        val name: String,
        val isSetOperator: Boolean = false,
        val isComparisionOperator: Boolean = false,
        val gridMatAndMat: (GridMat, GridMat, MatMatch) -> GridMat,
        val scalarAndGridMat: (Scalar, GridMat) -> GridMat,
        val gridMatAndScalar: (GridMat, Scalar) -> GridMat,
        val scalarAndScalar: (Scalar, Scalar) -> Scalar
) {
    fun eval(lhs: Value, rhs: Value): Value {
        // TODO: Handle bool modifier (rhs will be BoolConvert?)
        return when {
            lhs is GridMat && rhs is GridMat -> {
                val mm = MatMatch.oneToOne(lhs, rhs, false, listOf(Metric.nameLabel))
                        ?: throw PromQLException("Lhs and Rhs does not match: lhs($lhs), rhs($rhs)")
                gridMatAndMat(lhs, rhs, mm)
            }
            lhs is Scalar && rhs is GridMat -> scalarAndGridMat(lhs, rhs)
            lhs is GridMat && rhs is Scalar -> gridMatAndScalar(lhs, rhs)
            lhs is Scalar && rhs is Scalar -> scalarAndScalar(lhs, rhs)
            else -> throw RuntimeException("never here")
        }
    }

    companion object {
        private fun filterMetricNames(gridMat: GridMat): Array<Metric> {
            return Array(gridMat.metrics.size) { gridMat.metrics[it].filter(listOf(), listOf(Metric.nameLabel)) }
        }

        private fun simpleOp(fn: (lvals: DoubleArray, rvals: DoubleArray) -> DoubleArray): (GridMat, GridMat, MatMatch) -> GridMat {
            return { l, r, mm ->
                mm.apply(fn)
            }
        }

        private fun scalaMat(fn: (scalar: Scalar, mat: DoubleArray) -> DoubleArray): (Scalar, GridMat) -> GridMat {
            return { s, m ->
                val values = mutableListOf<DoubleArray>()
                for (row in m.values) {
                    values.add(fn(s, row))
                }
                GridMat(filterMetricNames(m), m.timestamps, values)
            }
        }

        private fun matScala(fn: (mat: DoubleArray, scalar: Scalar) -> DoubleArray): (GridMat, Scalar) -> GridMat {
            return { m, s ->
                val values = mutableListOf<DoubleArray>()
                for (row in m.values) {
                    values.add(fn(row, s))
                }
                GridMat(filterMetricNames(m), m.timestamps, values)
            }
        }

        val builtins = listOf(
                BinaryOp("+",
                        gridMatAndMat = simpleOp { lvals, rvals ->
                            val resvals = DoubleArray(lvals.size)
                            for (i in 0..(lvals.size - 1)) {
                                resvals[i] = lvals[i] + rvals[i]
                            }
                            resvals
                        },
                        scalarAndGridMat = scalaMat { s, m ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = sval + m[i]
                            }
                            resvals
                        },
                        gridMatAndScalar = matScala { m, s ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = m[i] + sval
                            }
                            resvals
                        },
                        scalarAndScalar = { s1, s2 ->
                            Scalar(s1.value + s2.value)
                        }
                ),
                BinaryOp("-",
                        gridMatAndMat = simpleOp { lvals, rvals ->
                            val resvals = DoubleArray(lvals.size)
                            for (i in 0..(lvals.size - 1)) {
                                resvals[i] = lvals[i] - rvals[i]
                            }
                            resvals
                        },
                        scalarAndGridMat = scalaMat { s, m ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = sval - m[i]
                            }
                            resvals
                        },
                        gridMatAndScalar = matScala { m, s ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = m[i] - sval
                            }
                            resvals
                        },
                        scalarAndScalar = { s1, s2 ->
                            Scalar(s1.value - s2.value)
                        }
                ),
                BinaryOp("*",
                        gridMatAndMat = simpleOp { lvals, rvals ->
                            val resvals = DoubleArray(lvals.size)
                            for (i in 0..(lvals.size - 1)) {
                                resvals[i] = lvals[i] * rvals[i]
                            }
                            resvals
                        },
                        scalarAndGridMat = scalaMat { s, m ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = sval * m[i]
                            }
                            resvals
                        },
                        gridMatAndScalar = matScala { m, s ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = m[i] * sval
                            }
                            resvals
                        },
                        scalarAndScalar = { s1, s2 ->
                            Scalar(s1.value * s2.value)
                        }
                ),
                BinaryOp("/",
                        gridMatAndMat = simpleOp { lvals, rvals ->
                            val resvals = DoubleArray(lvals.size)
                            for (i in 0..(lvals.size - 1)) {
                                if (rvals[i] == 0.0) {
                                    val lv = lvals[i]
                                    resvals[i] = when {
                                        lv == 0.0 -> Double.NaN
                                        lv > 0.0 -> Double.POSITIVE_INFINITY
                                        else -> Double.NEGATIVE_INFINITY
                                    }
                                } else {
                                    resvals[i] = lvals[i] / rvals[i]
                                }
                            }
                            resvals
                        },
                        scalarAndGridMat = scalaMat { s, m ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                if (m[i] == 0.0) {
                                    resvals[i] = when {
                                        sval == 0.0 -> Double.NaN
                                        sval > 0.0 -> Double.POSITIVE_INFINITY
                                        else -> Double.NEGATIVE_INFINITY
                                    }
                                } else {
                                    resvals[i] = sval / m[i]
                                }
                            }
                            resvals
                        },
                        gridMatAndScalar = matScala { m, s ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                if (sval == 0.0) {
                                    val lv = m[i]
                                    resvals[i] = when {
                                        lv == 0.0 -> Double.NaN
                                        lv > 0.0 -> Double.POSITIVE_INFINITY
                                        else -> Double.NEGATIVE_INFINITY
                                    }
                                } else {
                                    resvals[i] = m[i] / sval
                                }
                            }
                            resvals
                        },
                        scalarAndScalar = { s1, s2 ->
                            if (s2.value == 0.0) {
                                when {
                                    s1.value == 0.0 -> Scalar(Double.NaN)
                                    s1.value > 0.0 -> Scalar(Double.POSITIVE_INFINITY)
                                    else -> Scalar(Double.NEGATIVE_INFINITY)
                                }
                            } else {
                                Scalar(s1.value / s2.value)
                            }
                        }
                ),
                BinaryOp("%",
                        gridMatAndMat = simpleOp { lvals, rvals ->
                            val resvals = DoubleArray(lvals.size)
                            for (i in 0..(lvals.size - 1)) {
                                resvals[i] = lvals[i] % rvals[i]
                            }
                            resvals
                        },
                        scalarAndGridMat = scalaMat { s, m ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = sval % m[i]
                            }
                            resvals
                        },
                        gridMatAndScalar = matScala { m, s ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = m[i] % sval
                            }
                            resvals
                        },
                        scalarAndScalar = { s1, s2 ->
                            Scalar(s1.value % s2.value)
                        }
                )//,
//                BinaryOp("^") { l, r, mm ->
//                    l
//                },
//                BinaryOp("==", isComparisionOperator = true) { l, r, mm ->
//                    l
//                },
//                BinaryOp("!=", isComparisionOperator = true) { l, r, mm ->
//                    l
//                },
//                BinaryOp(">", isComparisionOperator = true) { l, r, mm ->
//                    l
//                },
//                BinaryOp("<", isComparisionOperator = true) { l, r, mm ->
//                    l
//                },
//                BinaryOp(">=", isComparisionOperator = true) { l, r, mm ->
//                    l
//                },
//                BinaryOp("<=", isComparisionOperator = true) { l, r, mm ->
//                    l
//                },
//                BinaryOp("and", isSetOperator = true) { l, r, mm ->
//                    l
//                },
//                BinaryOp("or", isSetOperator = true) { l, r, mm ->
//                    l
//                },
//                BinaryOp("unless", isSetOperator = true) { l, r, mm ->
//                    l
//                }
        )
    }
}