package epimetheus.pkg.promql

import epimetheus.model.*
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.LongArraySet

abstract class BinaryOp {
    abstract val name: String
    abstract val shouldDropMetricName: Boolean
    abstract val isSetOperator: Boolean
    abstract fun eval(lhs: Value, rhs: Value, matching: VectorMatching): Value

    protected fun resultMetric(lhsMet: Metric, rhsMet: Metric, matching: VectorMatching): Metric {
        val mb = lhsMet.builder()
        if (shouldDropMetricName) {
            mb.remove(Metric.nameLabel)
        }
        if (matching.card == VectorMatchingCardinality.OneToOne) {
            if (matching.on) {
                mb.removeIf { k, v ->
                    !matching.matchingLabels.contains(k)
                }
            } else {
                mb.removeIf { k, v ->
                    matching.matchingLabels.contains(k)
                }
            }
        }
        for (inc in matching.include) {
            val incMet = rhsMet.get(inc)
            mb.putOrRemove(inc, incMet)
        }
        return mb.build()
    }

    data class ArithAndLogical(
            override val name: String,
            override val shouldDropMetricName: Boolean = false, // true when +, -, *, /, %
            val opFn: (DoubleArray, DoubleArray) -> DoubleArray
    ) : BinaryOp() {
        override val isSetOperator = false

        override fun eval(lhs: Value, rhs: Value, matching: VectorMatching): Value {
            // TODO: Handle bool modifier (rhs will be BoolConvert?)
            return when {
                lhs is GridMat && rhs is GridMat -> evalMatMat(lhs, rhs, matching)
                lhs is Scalar && rhs is GridMat -> {
                    val l = DoubleArray(rhs.timestamps.size) { lhs.value }
                    val res = rhs.mapRows { _, _, vs ->
                        opFn(l, vs)
                    }
                    if (shouldDropMetricName) {
                        res.dropMetricName()
                    } else {
                        res
                    }
                }
                lhs is GridMat && rhs is Scalar -> {
                    val r = DoubleArray(lhs.timestamps.size) { rhs.value }
                    val res = lhs.mapRows { _, _, vs ->
                        opFn(vs, r)
                    }
                    if (shouldDropMetricName) {
                        res.dropMetricName()
                    } else {
                        res
                    }
                }
                lhs is Scalar && rhs is Scalar -> {
                    Scalar(opFn(doubleArrayOf(lhs.value), doubleArrayOf(rhs.value)).first())
                }
                else -> throw RuntimeException("never here")
            }
        }

        private fun evalMatMat(oLhs: GridMat, oRhs: GridMat, matching: VectorMatching): Value {
            if (matching.card == VectorMatchingCardinality.ManyToMany) {
                throw PromQLException("many-to-many only allowed for set operators")
            }

            // The control flow below handles one-to-one or many-to-one matching.
            // For one-to-many, swap sidedness and account for the swap when calculating
            // values.
            val sideSwapped = matching.card == VectorMatchingCardinality.OneToMany
            val lhs = if (sideSwapped) oRhs else oLhs
            val rhs = if (sideSwapped) oLhs else oRhs
            val rightSigs = Long2IntOpenHashMap(rhs.metrics.size)
            for (i in 0 until rhs.metrics.size) {
                val sig = rhs.metrics[i].filteredFingerprint(matching.on, matching.matchingLabels, shouldDropMetricName)
                if (rightSigs.containsKey(sig)) {
                    throw PromQLException("many-to-many matching not allowed: matching labels must be unique on one side: duplicates ${rhs.metrics[i]}")
                }
                rightSigs[sig] = i
            }

            val matchedSigs = Long2ObjectOpenHashMap<LongArraySet>(rightSigs.size)

            val resultMetrics = mutableListOf<Metric>()
            val resultValues = mutableListOf<DoubleArray>()

            for (i in 0 until lhs.metrics.size) {
                val sig = lhs.metrics[i].filteredFingerprint(matching.on, matching.matchingLabels, shouldDropMetricName)
                if (!rightSigs.containsKey(sig)) {
                    continue // or set stale?
                }
                val rs = rightSigs[sig]
                val vals = opFn(lhs.values[i], rhs.values[rs])
                val metric = resultMetric(lhs.metrics[i], rhs.metrics[rs], matching)

                val insertedSigs = matchedSigs[sig]
                if (matching.card == VectorMatchingCardinality.OneToOne) {
                    if (insertedSigs != null) {
                        throw PromQLException("multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)")
                    }
                    matchedSigs[sig] = LongArraySet()
                } else {
                    val insertSig = metric.fingerprint()

                    if (insertedSigs == null) {
                        val las = LongArraySet()
                        las.add(insertSig)
                        matchedSigs[sig] = las
                    } else if (insertedSigs.contains(insertSig)) {
                        throw PromQLException("multiple matches for labels: grouping labels must ensure unique matches")
                    } else {
                        insertedSigs.add(insertSig)
                    }
                }
                resultMetrics.add(metric)
                resultValues.add(vals)
            }
            return GridMat.withSortting(resultMetrics, oLhs.timestamps, resultValues.toList())
        }
    }

    data class SetOp(
            override val name: String,
            val opFn: (GridMat, GridMat, VectorMatching) -> GridMat
    ) : BinaryOp() {
        override val shouldDropMetricName = false
        override val isSetOperator = true

        override fun eval(lhs: Value, rhs: Value, matching: VectorMatching): Value {
            if (lhs !is GridMat || rhs !is GridMat) {
                throw PromQLException("$name defined only between instant vectors")
            }
            if (matching.card != VectorMatchingCardinality.ManyToMany) {
                throw PromQLException("set operations must only use many-to-many matching")
            }
            return opFn(lhs, rhs, matching)
        }
    }


    companion object {
        val builtins = listOf(
                ArithAndLogical("+", shouldDropMetricName = true) { lhs, rhs ->
                    val r = DoubleArray(lhs.size)
                    for (i in 0 until r.size) {
                        r[i] = lhs[i] + rhs[i]
                    }
                    r
                },
                ArithAndLogical("-", shouldDropMetricName = true) { lhs, rhs ->
                    val r = DoubleArray(lhs.size)
                    for (i in 0 until r.size) {
                        r[i] = lhs[i] - rhs[i]
                    }
                    r
                },
                ArithAndLogical("*", shouldDropMetricName = true) { lhs, rhs ->
                    val r = DoubleArray(lhs.size)
                    for (i in 0 until r.size) {
                        r[i] = lhs[i] * rhs[i]
                    }
                    r
                },
                ArithAndLogical("/", shouldDropMetricName = true) { lhs, rhs ->
                    val r = DoubleArray(lhs.size)
                    for (i in 0 until r.size) {
                        r[i] = lhs[i] / rhs[i]
                    }
                    r
                },
                ArithAndLogical("%", shouldDropMetricName = true) { lhs, rhs ->
                    val r = DoubleArray(lhs.size)
                    for (i in 0 until r.size) {
                        r[i] = lhs[i] % rhs[i]
                    }
                    r
                },
                ArithAndLogical("^") { lhs, rhs ->
                    DoubleArray(lhs.size) { i ->
                        Math.pow(lhs[i], rhs[i])
                    }
                },
                ArithAndLogical("==") { lhs, rhs ->
                    DoubleArray(lhs.size) { i ->
                        if (lhs[i] == rhs[i]) {
                            lhs[i]
                        } else {
                            Mat.StaleValue
                        }
                    }
                },
                ArithAndLogical("!=") { lhs, rhs ->
                    DoubleArray(lhs.size) { i ->
                        if (lhs[i] != rhs[i]) {
                            lhs[i]
                        } else {
                            Mat.StaleValue
                        }
                    }
                },
                ArithAndLogical(">") { lhs, rhs ->
                    DoubleArray(lhs.size) { i ->
                        if (lhs[i] > rhs[i]) {
                            lhs[i]
                        } else {
                            Mat.StaleValue
                        }
                    }
                },
                ArithAndLogical("<") { lhs, rhs ->
                    DoubleArray(lhs.size) { i ->
                        if (lhs[i] < rhs[i]) {
                            lhs[i]
                        } else {
                            Mat.StaleValue
                        }
                    }
                },
                ArithAndLogical(">=") { lhs, rhs ->
                    DoubleArray(lhs.size) { i ->
                        if (lhs[i] >= rhs[i]) {
                            lhs[i]
                        } else {
                            Mat.StaleValue
                        }
                    }
                },
                ArithAndLogical("<=") { lhs, rhs ->
                    DoubleArray(lhs.size) { i ->
                        if (lhs[i] <= rhs[i]) {
                            lhs[i]
                        } else {
                            Mat.StaleValue
                        }
                    }
                },
                SetOp("and") { lhs, rhs, matching ->
                    val rightSigs = LongArraySet(rhs.metrics.size)
                    rhs.metrics.forEach { met -> rightSigs.add(met.filteredFingerprint(matching.on, matching.matchingLabels, true)) }
                    val resultMetrics = mutableListOf<Metric>()
                    val resultValues = mutableListOf<DoubleArray>()
                    lhs.metrics.forEachIndexed { index, met ->
                        val fp = met.filteredFingerprint(matching.on, matching.matchingLabels, true)
                        if (rightSigs.contains(fp)) {
                            resultMetrics.add(met)
                            resultValues.add(lhs.values[index])
                        }
                    }
                    GridMat(resultMetrics.toTypedArray(), lhs.timestamps, resultValues)
                },
                SetOp("or") { lhs, rhs, matching ->
                    val leftSigs = LongArraySet(lhs.metrics.size)
                    lhs.metrics.forEach { met -> leftSigs.add(met.filteredFingerprint(matching.on, matching.matchingLabels, true)) }
                    val resultMetrics = MutableList(lhs.metrics.size) { lhs.metrics[it] }
                    val resultValues = MutableList(lhs.values.size) { lhs.values[it] }
                    rhs.metrics.forEachIndexed { index, met ->
                        val fp = met.filteredFingerprint(matching.on, matching.matchingLabels, true)
                        if (!leftSigs.contains(fp)) {
                            resultMetrics.add(met)
                            resultValues.add(rhs.values[index])
                        }
                    }
                    GridMat.withSortting(resultMetrics, lhs.timestamps, resultValues)
                },
                SetOp("unless") { lhs, rhs, matching ->
                    val rightSigs = LongArraySet(lhs.metrics.size)
                    rhs.metrics.forEach { met -> rightSigs.add(met.filteredFingerprint(matching.on, matching.matchingLabels, true)) }
                    val resultMetrics = mutableListOf<Metric>()
                    val resultValues = mutableListOf<DoubleArray>()
                    lhs.metrics.forEachIndexed { index, met ->
                        val fp = met.filteredFingerprint(matching.on, matching.matchingLabels, true)
                        if (!rightSigs.contains(fp)) {
                            resultMetrics.add(met)
                            resultValues.add(lhs.values[index])
                        }
                    }
                    GridMat(resultMetrics.toTypedArray(), lhs.timestamps, resultValues)
                }
        )
    }
}