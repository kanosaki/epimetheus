package epimetheus.engine.plan

import epimetheus.model.Metric
import epimetheus.pkg.promql.*
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.LongArraySet


class BinOpPlanner(val binding: Map<String, BOp>) {
    fun plan(planner: Planner, ast: BinaryCall, ctx: PlannerContext): PlanNode {
        val lhs = planner.plan(ast.lhs, ctx)
        val rhs = planner.plan(ast.rhs, ctx)
        val op = binding[ast.op.name] ?: throw PromQLException("binary operator '${ast.op.name}' is not defined")
        return when (op) {
            is NumericBinOp -> when {
                lhs is InstantNode && rhs is InstantNode -> {
                    planMatMat(op, ast.matching, lhs, rhs)
                }
                lhs is ScalarLiteralNode && rhs is InstantNode -> {
                    val mp = rhs.metric
                    if (mp is FixedMetric) {
                        val metrics = if (op.shouldDropMetricName) {
                            mp.metrics.map { it.filterWithout(true, listOf()) }
                        } else {
                            mp.metrics
                        }
                        BinOpArithScalarMatNode(FixedMetric(metrics), lhs, rhs, op) // TODO: attach op
                    } else {
                        BinOpArithScalarMatNode(VariableMetric, lhs, rhs, op)
                    }
                }
                lhs is InstantNode && rhs is ScalarLiteralNode -> {
                    val mp = lhs.metric
                    if (mp is FixedMetric) {
                        val metrics = if (op.shouldDropMetricName) {
                            mp.metrics.map { it.filterWithout(true, listOf()) }
                        } else {
                            mp.metrics
                        }
                        BinOpArithMatScalarNode(FixedMetric(metrics), lhs, rhs, op) // TODO: attach op
                    } else {
                        BinOpArithMatScalarNode(VariableMetric, lhs, rhs, op)
                    }
                }
                lhs is ScalarLiteralNode && rhs is ScalarLiteralNode -> {
                    // constant folding
                    val res = op.fn(lhs.value, rhs.value)
                    ScalarLiteralNode(res)
                }
                else -> {
                    throw PromQLException("unsupported binary operation(op: ${op.name}) between $lhs and $rhs")
                }
            }
            is SetBinOp -> when {
                lhs is InstantNode && rhs is InstantNode -> {
                    return BinOpDynamicNode(VariableMetric, lhs, rhs, op, ast.matching)
                }
                else -> {
                    throw PromQLException("${op.name} defined only between instant vectors")
                }
            }
            else -> TODO()
        }
    }


    fun planMatMat(op: NumericBinOp, matching: VectorMatching, oLhs: InstantNode, oRhs: InstantNode): InstantNode {
        if (matching.card == VectorMatchingCardinality.ManyToMany) {
            throw PromQLException("many-to-many only allowed for set operators")
        }

        val lhsMetrics = oLhs.metric as? FixedMetric
        val rhsMetrics = oRhs.metric as? FixedMetric
        if (lhsMetrics == null || rhsMetrics == null) {
            return BinOpDynamicNode(VariableMetric, oLhs, oRhs, op, matching)
        }

        val matchVector = computeMatching(matching, lhsMetrics.metrics, rhsMetrics.metrics, op.shouldDropMetricName)
        return BinOpArithMatMatNode(FixedMetric(matchVector.first), matchVector.second, oLhs, oRhs, op)
    }

    companion object {
        fun resultMetric(shouldDropMetricName: Boolean, lhsMet: Metric, rhsMet: Metric, matching: VectorMatching): Metric {
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

        fun computeMatching(matching: VectorMatching, lhsMetrics: List<Metric>, rhsMetrics: List<Metric>, shouldDropMetricName: Boolean): Pair<List<Metric>, List<IntArray>> {
            val sideSwapped = matching.card == VectorMatchingCardinality.OneToMany
            val lhs = if (sideSwapped) rhsMetrics else lhsMetrics
            val rhs = if (sideSwapped) lhsMetrics else rhsMetrics
            val rightSigs = Long2IntOpenHashMap(rhs.size)
            for (i in 0 until rhs.size) {
                val sig = rhs[i].filteredFingerprint(matching.on, matching.matchingLabels, shouldDropMetricName)
                if (rightSigs.containsKey(sig)) {
                    throw PromQLException("many-to-many matching not allowed: matching labels must be unique on one side: duplicates ${rhs[i]}")
                }
                rightSigs[sig] = i
            }

            val matchedSigs = Long2ObjectOpenHashMap<LongArraySet>(rightSigs.size)

            val resultMetrics = mutableListOf<Metric>()
            val metMatching = mutableListOf<IntArray>() // matched index of lhs, index of rhs

            for (i in 0 until lhs.size) {
                val sig = lhs[i].filteredFingerprint(matching.on, matching.matchingLabels, shouldDropMetricName)
                if (!rightSigs.containsKey(sig)) {
                    continue // or set stale?
                }
                val rs = rightSigs[sig]
                val metric = resultMetric(shouldDropMetricName, lhs[i], rhs[rs], matching)

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
                if (sideSwapped) {// TODO: check, not adopted eval version, but we should re-swap here
                    metMatching.add(intArrayOf(rs, i))
                } else {
                    metMatching.add(intArrayOf(i, rs))
                }
                resultMetrics.add(metric)
            }
            return resultMetrics to metMatching
        }

    }
}


