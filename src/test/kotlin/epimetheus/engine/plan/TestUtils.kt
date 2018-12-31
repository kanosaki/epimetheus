package epimetheus.engine.plan

import epimetheus.model.*
import epimetheus.model.TestUtils.compareDouble
import org.junit.jupiter.api.fail
import kotlin.test.assertEquals

object TestUtils {
    fun toRuntimeValue(v: Value): RuntimeValue {
        if (v is RuntimeValue) {
            return v
        }
        return when (v) {
            is GridMat -> {
                val timestamps = LongSlice.wrap(v.timestamps.toLongArray())
                RPointMatrix(v.metrics.toList(), v.values.map { RPoints(timestamps, DoubleSlice.wrap(it)) }, TimeFrames.instant(0)) // TODO: fix frames
            }
            is RangeGridMat -> {
                RRangeMatrix(
                        v.metrics,
                        v.series.map { chunk ->
                            RRanges(chunk.map { RPoints(LongSlice.wrap(it.first), DoubleSlice.wrap(it.second)) })
                        },
                        v.timestamps,
                        v.windowSize,
                        v.offset)
            }
            is Scalar -> RScalar(v.value)
            is StringValue -> RString(v.value)
            else -> TODO("not implemented for ${v.javaClass}")
        }
    }

    fun assertRuntimeValueEquals(v1: RuntimeValue, v2: RuntimeValue, allowNonDetComparision: Boolean = false, prune: Boolean = false, msg: String = "") {
        if (v1 is RPointMatrix && v2 is RPointMatrix) {
            if (prune) {
                assertRPointMatrixEquals(v1.prune(), v2.prune(), allowNonDetComparision, msg)
            } else {
                assertRPointMatrixEquals(v1, v2, allowNonDetComparision, msg)
            }
        } else if (v1 is RScalar && v2 is RScalar) {
            if (!compareDouble(v1.value, v2.value, allowNonDetComparision)) {
                fail("value not equal: $v1 != $v2 (allowNonDetComparision=$allowNonDetComparision)")
            }
        } else if (v1 is RRangeMatrix && v2 is RRangeMatrix) {
            TODO("implement")
        } else {
            assertEquals(v1, v2)
        }
    }

    private fun assertRPointMatrixEquals(v1: RPointMatrix, v2: RPointMatrix, allowNonDetComparsion: Boolean = false, msg: String = "") {
        assertEquals(v1.metrics, v2.metrics, "metrics mismatch: $msg")
        for (i in 0 until v1.series.size) {
            val s1 = v1.series[i]
            val s2 = v2.series[i]
            for (j in 0 until s1.values.size) {
                val x1 = s1.values[j]
                val x2 = s2.values[j]
                if (!compareDouble(x1, x2, allowNonDetComparsion)) {
                    fail("Value not match at $i(${v1.metrics[i]})\n at $j $x1(${x1.toRawBits()}) != $x2(${x2.toRawBits()})\n expected: $s1 \n actual: $s2 $msg")
                }
            }
        }
    }
}

