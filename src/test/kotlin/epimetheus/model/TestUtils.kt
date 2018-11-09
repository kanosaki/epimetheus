package epimetheus.model

import kotlin.test.assertEquals
import kotlin.test.fail

object TestUtils {
    const val DOUBLE_COMPARE_DELTA = 10e-9

    fun assertValueEquals(m1: Value, m2: Value, allowNonDetComparsion: Boolean = false, prune: Boolean = false) {
        if (m1 is Mat && m2 is Mat) {
            return if (prune) {
                assertMatEquals(m1.prune(), m2.prune(), allowNonDetComparsion)
            } else {
                assertMatEquals(m1, m2, allowNonDetComparsion)
            }
        } else if (m1 is Scalar && m2 is Scalar) {
            if (!compareDouble(m1.value, m2.value, allowNonDetComparsion)) {
                fail("Scalar not match: $m1 != $m2")
            }
        } else {
            fail("Type mismatch: $m1 != $m2")
        }
    }

    private fun compareDouble(x1: Double, x2: Double, allowNonDetComparsion: Boolean = false): Boolean {
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

    fun assertMatEquals(m1: Mat, m2: Mat, allowNonDetComparsion: Boolean = false) {
        assertEquals(m1.timestamps, m2.timestamps, "Timestamp mismatch")
        if (m1.metrics.size != m2.metrics.size) {
            fail("metric number mismatch: : expected: ${m1.metrics.size} != actual: ${m2.metrics.size}")
        }
        for (i in 0 until m1.metrics.size) {
            if (m1.metrics[i] != m2.metrics[i]) {
                fail("metric mismatch: at $i: expected: ${m1.metrics[i]} != actual: ${m2.metrics[i]}")
            }
        }
        if (m1.values.size != m2.values.size) {
            fail("values size mismatch: expected: ${m1.values.size} != actual: ${m2.values.size}")
        }
        for (i in 0 until m1.values.size) {
            val v1 = m1.values[i]
            val v2 = m2.values[i]
            for (j in 0 until v1.size) {
                val x1 = v1[j]
                val x2 = v2[j]
                if (!compareDouble(x1, x2, allowNonDetComparsion)) {
                    fail("Value not match at $i(${m1.metrics[i]})\n at $j $x1(${x1.toRawBits()}) != $x2(${x2.toRawBits()})\n expected: ${v1.toList()} \n actual: ${v2.toList()}")
                }
            }
        }
    }
}