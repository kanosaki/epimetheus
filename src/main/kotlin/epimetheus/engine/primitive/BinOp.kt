package epimetheus.engine.primitive

import epimetheus.model.Mat
import epimetheus.model.Metric
import epimetheus.pkg.promql.VectorMatching

interface BOp {
    val name: String

    companion object {
        private inline fun nanGuard(isNan: Boolean, fn: () -> Double): Double {
            return if (isNan) {
                Double.NaN
            } else {
                fn()
            }
        }

        private fun logical(pred: Boolean, v: Double): Double {
            return if (pred) {
                v
            } else {
                Mat.StaleValue
            }
        }

        private val numericBuiltins = listOf(
                NumericBinOp("+", true) { l, r ->
                    l + r
                },
                NumericBinOp("-", true) { l, r ->
                    l - r
                },
                NumericBinOp("*", true) { l, r ->
                    l * r
                },
                NumericBinOp("/", true) { l, r ->
                    nanGuard(l == 0.0) { l / r }
                },
                NumericBinOp("%", true) { l, r ->
                    nanGuard(l == 0.0) { l % r }
                },
                NumericBinOp("^") { l, r ->
                    Math.pow(l, r)
                },
                NumericBinOp("==") { l, r ->
                    logical(l == r, l)
                },
                NumericBinOp("!=") { l, r ->
                    logical(l != r, l)
                },
                NumericBinOp(">") { l, r ->
                    logical(l > r, l)
                },
                NumericBinOp("<") { l, r ->
                    logical(l < r, l)
                },
                NumericBinOp(">=") { l, r ->
                    logical(l >= r, l)
                },
                NumericBinOp("<=") { l, r ->
                    logical(l <= r, l)
                }
        )
        private val setBuiltins = listOf(
                SetBinOp("and") { lm, rm, matching ->
                    val rightSigs = HashSet(rm.map { it.filteredFingerprint(matching.on, matching.matchingLabels, true) })
                    val resultMetrics = lm.mapIndexedNotNull { index, met ->
                        val fp = met.filteredFingerprint(matching.on, matching.matchingLabels, true)
                        if (rightSigs.contains(fp)) {
                            index
                        } else {
                            null
                        }
                    }
                    resultMetrics to listOf()
                },
                SetBinOp("or") { lm, rm, matching ->
                    val leftSigs = HashSet(lm.map { it.filteredFingerprint(matching.on, matching.matchingLabels, true) })
                    val resultMetrics = rm.mapIndexedNotNull { index, met ->
                        val fp = met.filteredFingerprint(matching.on, matching.matchingLabels, true)
                        if (!leftSigs.contains(fp)) {
                            index
                        } else {
                            null
                        }
                    }
                    (0 until lm.size).toList() to resultMetrics
                },
                SetBinOp("unless") { lm, rm, matching ->
                    val rightSigs = HashSet(rm.map { it.filteredFingerprint(matching.on, matching.matchingLabels, true) })
                    val resultMetrics = lm.mapIndexedNotNull { index, met ->
                        val fp = met.filteredFingerprint(matching.on, matching.matchingLabels, true)
                        if (!rightSigs.contains(fp)) {
                            index
                        } else {
                            null
                        }
                    }
                    resultMetrics to listOf()
                }
        )
        val builtinMap = (numericBuiltins + setBuiltins).map { it.name to it }.toMap()
    }
}

data class NumericBinOp(override val name: String, val shouldDropMetricName: Boolean = false, val fn: (Double, Double) -> Double) : BOp {

}

data class SetBinOp(override val name: String, val fn: (List<Metric>, List<Metric>, VectorMatching) -> Pair<List<Int>, List<Int>>) : BOp

