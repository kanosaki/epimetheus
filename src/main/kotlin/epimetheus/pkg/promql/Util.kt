package epimetheus.pkg.promql

import java.util.*

object Utils {
    fun isValidLabelName(cs: String): Boolean {
        // Refer: PromQLLexer .dropMetricName()
        // NAME
        // : [a-zA-Z_] [a-zA-Z0-9_]*;
        for (i in 0 until cs.length) {
            val c = cs[i]
            if (i == 0) {
                if (!((c in 'a'..'z') || (c in 'A'..'Z') || c == '_')) {
                    return false
                }
            } else {
                if (!((c in 'a'..'z') || (c in 'A'..'Z') || (c in '0'..'9') || c == '_')) {
                    return false
                }
            }
        }
        return true
    }

    fun fmtDouble(d: Double): String {
        return if (d == d.toLong().toDouble())
            String.format("%d", d.toLong())
        else
            String.format("%s", d)
    }

    fun quantile(q: Double, vs: DoubleArray): Double {
        if (vs.isEmpty()) {
            return Double.NaN
        }
        if (q < 0) {
            return Double.NEGATIVE_INFINITY
        }
        if (q > 1) {
            return Double.POSITIVE_INFINITY
        }
        Arrays.sort(vs)
        val n = vs.size.toDouble()
        val rank = q * (n - 1)
        val lowerIndex = Math.max(0.0, Math.floor(rank))
        val upperIndex = Math.min(n - 1, lowerIndex + 1)
        val weight = rank - Math.floor(rank)
        return vs[lowerIndex.toInt()] * (1 - weight) + vs[upperIndex.toInt()] * weight
    }
}