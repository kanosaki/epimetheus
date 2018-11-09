package epimetheus.pkg.promql

import epimetheus.model.*

data class BinaryOp(
        val name: String,
        val isSetOperator: Boolean = false,
        val isComparisionOperator: Boolean = false,
        val matAndMat: (Mat, Mat, MatMatch) -> Mat,
        val scalarAndMat: (Scalar, Mat) -> Mat,
        val matAndScalar: (Mat, Scalar) -> Mat,
        val scalarAndScalar: (Scalar, Scalar) -> Scalar
) {
    fun eval(lhs: Value, rhs: Value): Value {
        // TODO: Handle bool modifier (rhs will be BoolConvert?)
        return when {
            lhs is Mat && rhs is Mat -> {
                val mm = MatMatch.oneToOne(lhs, rhs, false, listOf(Metric.nameLabel))
                        ?: throw PromQLException("Lhs and Rhs does not match: lhs($lhs), rhs($rhs)")
                matAndMat(lhs, rhs, mm)
            }
            lhs is Scalar && rhs is Mat -> scalarAndMat(lhs, rhs)
            lhs is Mat && rhs is Scalar -> matAndScalar(lhs, rhs)
            lhs is Scalar && rhs is Scalar -> scalarAndScalar(lhs, rhs)
            else -> throw RuntimeException("never here")
        }
    }

    companion object {
        private fun filterMetricNames(mat: Mat): Array<Metric> {
            return Array(mat.metrics.size) { mat.metrics[it].filter(listOf(), listOf(Metric.nameLabel)) }
        }

        private fun simpleOp(fn: (lvals: DoubleArray, rvals: DoubleArray) -> DoubleArray): (Mat, Mat, MatMatch) -> Mat {
            return { l, r, mm ->
                mm.apply(fn)
            }
        }

        private fun scalaMat(fn: (scalar: Scalar, mat: DoubleArray) -> DoubleArray): (Scalar, Mat) -> Mat {
            return { s, m ->
                val values = mutableListOf<DoubleArray>()
                for (row in m.values) {
                    values.add(fn(s, row))
                }
                Mat(filterMetricNames(m), m.timestamps, values)
            }
        }

        private fun matScala(fn: (mat: DoubleArray, scalar: Scalar) -> DoubleArray): (Mat, Scalar) -> Mat {
            return { m, s ->
                val values = mutableListOf<DoubleArray>()
                for (row in m.values) {
                    values.add(fn(row, s))
                }
                Mat(filterMetricNames(m), m.timestamps, values)
            }
        }

        val builtins = listOf(
                BinaryOp("+",
                        matAndMat = simpleOp { lvals, rvals ->
                            val resvals = DoubleArray(lvals.size)
                            for (i in 0..(lvals.size - 1)) {
                                resvals[i] = lvals[i] + rvals[i]
                            }
                            resvals
                        },
                        scalarAndMat = scalaMat { s, m ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = sval + m[i]
                            }
                            resvals
                        },
                        matAndScalar = matScala { m, s ->
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
                        matAndMat = simpleOp { lvals, rvals ->
                            val resvals = DoubleArray(lvals.size)
                            for (i in 0..(lvals.size - 1)) {
                                resvals[i] = lvals[i] - rvals[i]
                            }
                            resvals
                        },
                        scalarAndMat = scalaMat { s, m ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = sval - m[i]
                            }
                            resvals
                        },
                        matAndScalar = matScala { m, s ->
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
                        matAndMat = simpleOp { lvals, rvals ->
                            val resvals = DoubleArray(lvals.size)
                            for (i in 0..(lvals.size - 1)) {
                                resvals[i] = lvals[i] * rvals[i]
                            }
                            resvals
                        },
                        scalarAndMat = scalaMat { s, m ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = sval * m[i]
                            }
                            resvals
                        },
                        matAndScalar = matScala { m, s ->
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
                        matAndMat = simpleOp { lvals, rvals ->
                            val resvals = DoubleArray(lvals.size)
                            for (i in 0..(lvals.size - 1)) {
                                if (rvals[i] == 0.0 || rvals[i] == -0.0) {
                                    resvals[i] = Double.NaN
                                } else {
                                    resvals[i] = lvals[i] / rvals[i]
                                }
                            }
                            resvals
                        },
                        scalarAndMat = scalaMat { s, m ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                if (m[i] == 0.0 || m[i] == -0.0) {
                                    resvals[i] = Double.NaN
                                } else {
                                    resvals[i] = sval / m[i]
                                }
                            }
                            resvals
                        },
                        matAndScalar = matScala { m, s ->
                            val sval = s.value
                            if (sval == 0.0 || sval == -0.0) {
                                return@matScala DoubleArray(m.size) { Double.NaN }
                            }
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = m[i] / sval
                            }
                            resvals
                        },
                        scalarAndScalar = { s1, s2 ->
                            if (s2.value == 0.0 || s2.value == -0.0) {
                                Scalar(Double.NaN)
                            } else {
                                Scalar(s1.value / s2.value)
                            }
                        }
                ),
                BinaryOp("%",
                        matAndMat = simpleOp { lvals, rvals ->
                            val resvals = DoubleArray(lvals.size)
                            for (i in 0..(lvals.size - 1)) {
                                resvals[i] = lvals[i] % rvals[i]
                            }
                            resvals
                        },
                        scalarAndMat = scalaMat { s, m ->
                            val sval = s.value
                            val resvals = DoubleArray(m.size)
                            for (i in 0..(m.size - 1)) {
                                resvals[i] = sval % m[i]
                            }
                            resvals
                        },
                        matAndScalar = matScala { m, s ->
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