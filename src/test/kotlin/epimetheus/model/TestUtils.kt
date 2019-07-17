package epimetheus.model

import epimetheus.engine.plan.TestUtils
import epimetheus.engine.plan.TestUtils.toRuntimeValue
import kotlin.test.assertEquals
import kotlin.test.fail

object TestUtils {
    const val DOUBLE_COMPARE_DELTA = 1e-6

    fun assertValueEquals(m1: Value, m2: Value, allowNonDetComparsion: Boolean = false, prune: Boolean = false, ordered: Boolean = false, msg: String = "") {
        TestUtils.assertRuntimeValueEquals(
                toRuntimeValue(m1),
                toRuntimeValue(m2),
                allowNonDetComparsion, prune, ordered, msg)
    }


    fun compareDouble(x1: Double, x2: Double, allowNonDetComparsion: Boolean = false): Boolean {
        if (x1.isFinite() && x2.isFinite()) {
            if (Math.abs(x1 - x2) < DOUBLE_COMPARE_DELTA) {
                return true
            }
        } else if (allowNonDetComparsion) {
            if (x1.isNaN() && x2.isNaN()
                    || x1 == Double.POSITIVE_INFINITY && x2 == Double.POSITIVE_INFINITY
                    || x1 == Double.NEGATIVE_INFINITY && x2 == Double.NEGATIVE_INFINITY) {
                return true
            }
        }
        return false
    }

    fun assertRangeMatEquals(m1: RangeGridMat, m2: RangeGridMat, allowNonDetComparsion: Boolean = false, memo: String = "") {
        assertEquals(m1.timestamps, m2.timestamps, "Timestamp mismatch $memo")
        assertEquals(m1.windowSize, m2.windowSize, "WindowSize mismatch $memo")
        if (m1.metrics.size != m2.metrics.size) {
            fail("metric number mismatch: : expected: ${m1.metrics.size} != actual: ${m2.metrics.size} $memo")
        }
        for (i in 0 until m1.metrics.size) {
            if (m1.metrics[i] != m2.metrics[i]) {
                fail("metric mismatch: at $i: expected: ${m1.metrics[i]} != actual: ${m2.metrics[i]} $memo")
            }
        }
        if (m1.series.size != m2.series.size) {
            fail("values size mismatch: expected: ${m1.series.size} != actual: ${m2.series.size} $memo")
        }
        if (m1.series.isEmpty()) {
            return
        }
        val firstSize = m1.series[0].size
        if (m1.series.any { it.size != firstSize } || m2.series.any { it.size != firstSize }) {
            fail("values size mismatch: expected: ${m1.series.map { it.size }} != actual: ${m2.series.map { it.size }} $m1 $m2 }} $memo")
        }
        for (i in 0 until m1.series.size) {
            val s1 = m1.series[i]
            val s2 = m2.series[i]
            for (j in 0 until s1.size) {
                val c1 = s1[j]
                val c2 = s2[j]
                assertEquals(c1.first.toList(), c2.first.toList(), "timestamps mismatch($i, $j): metric: ${m1.metrics[i]}, ${c1.first.toList()} != ${c2.first.toList()}")
                for (k in 0 until c1.second.size) {
                    val d1 = c1.second[k]
                    val d2 = c2.second[k]
                    if (!compareDouble(d1, d2, allowNonDetComparsion)) {
                        fail("Value not match at $i(${m1.metrics[i]})\n at $j $d1(${d1.toRawBits()}) != $d2(${d2.toRawBits()})\n expected: ${c1.second.toList()} \n actual: ${c2.second.toList()} $memo")
                    }
                }
            }
        }
        //if (m1.offset != m2.offset) {
        //    fail("RangeGridMat offset not match: $m1 != $m2 $memo")
        //}
    }

    fun assertMatEquals(m1: GridMat, m2: GridMat, allowNonDetComparsion: Boolean = false, memo: String = "") {
        assertEquals(m1.timestamps.toList(), m2.timestamps.toList(), "Timestamp mismatch $memo")
        if (m1.metrics.size != m2.metrics.size) {
            fail("metric number mismatch: : expected: ${m1.metrics.size} != actual: ${m2.metrics.size} $m1 $m2 $memo")
        }
        for (i in 0 until m1.metrics.size) {
            if (m1.metrics[i] != m2.metrics[i]) {
                fail("metric mismatch: at $i: expected: ${m1.metrics[i]} != actual: ${m2.metrics[i]} $memo")
            }
        }
        if (m1.values.size != m2.values.size) {
            fail("values size mismatch: expected: ${m1.values.size} != actual: ${m2.values.size} $memo")
        }
        for (i in 0 until m1.values.size) {
            val v1 = m1.values[i]
            val v2 = m2.values[i]
            for (j in 0 until v1.size) {
                val x1 = v1[j]
                val x2 = v2[j]
                if (!compareDouble(x1, x2, allowNonDetComparsion)) {
                    fail("Value not match at $i(${m1.metrics[i]})\n at $j $x1(${x1.toRawBits()}) != $x2(${x2.toRawBits()})\n expected: ${v1.toList()} \n actual: ${v2.toList()} $memo")
                }
            }
        }
        //if (m1.offset != m2.offset) {
        //    fail("GridMat offset not match: ${m1.offset} != ${m2.offset} $memo")
        //}
    }
}
