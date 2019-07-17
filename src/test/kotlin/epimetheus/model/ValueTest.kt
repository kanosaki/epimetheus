package epimetheus.model

import epimetheus.pkg.promql.PromQLException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class RPointMatrixBuilderTest {
    @Test
    fun testAsIs() {
        val b = RPointMatrixBuilder(TimeFrames.instant(0))
        b.add(Metric.of("c"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(3.0))))
        b.add(Metric.of("b"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(2.0))))
        b.add(Metric.of("a"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(1.0))))
        val expected = RPointMatrix.of(
                TimeFrames.instant(0),
                Metric.of("c") to listOf(3.0),
                Metric.of("b") to listOf(2.0),
                Metric.of("a") to listOf(1.0),
                sort = false
        )
        TestUtils.assertValueEquals(b.build(), expected, ordered = true)
    }

    @Test
    fun testSortByFingerprint() {
        val b = RPointMatrixBuilder(TimeFrames.instant(0), sort = true)
        b.add(Metric.of("c"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(3.0))))
        b.add(Metric.of("b"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(2.0))))
        b.add(Metric.of("a"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(1.0))))
        val expected = RPointMatrix.of(
                TimeFrames.instant(0),
                Metric.of("a") to listOf(1.0),
                Metric.of("c") to listOf(3.0),
                Metric.of("b") to listOf(2.0),
                sort = false
        )
        val sorted = expected.sortSeries()
        TestUtils.assertValueEquals(b.build(), sorted, ordered = true)
    }

    @Test
    fun testSortByValueDesc() {
        val b = RPointMatrixBuilder(TimeFrames.instant(0))
        b.add(Metric.of("b"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(2.0))))
        b.add(Metric.of("c"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(3.0))))
        b.add(Metric.of("a"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(1.0))))
        val expected = RPointMatrix.of(
                TimeFrames.instant(0),
                Metric.of("c") to listOf(3.0),
                Metric.of("b") to listOf(2.0),
                Metric.of("a") to listOf(1.0),
                sort = false
        )
        TestUtils.assertValueEquals(b.sortByValue(true).build(), expected, ordered = true)
    }

    @Test
    fun testSortByValueAsc() {
        val b = RPointMatrixBuilder(TimeFrames.instant(0))
        b.add(Metric.of("b"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(2.0))))
        b.add(Metric.of("c"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(3.0))))
        b.add(Metric.of("a"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(1.0))))
        val expected = RPointMatrix.of(
                TimeFrames.instant(0),
                Metric.of("a") to listOf(1.0),
                Metric.of("b") to listOf(2.0),
                Metric.of("c") to listOf(3.0),
                sort = false
        )
        TestUtils.assertValueEquals(b.sortByValue(false).build(), expected, ordered = true)
    }

    @Test
    fun testRejectDuplication() {
        val b = RPointMatrixBuilder(TimeFrames.instant(0), sort = true, allowDuplication = false)
        b.add(Metric.of("a"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(1.0))))
        assertThrows<PromQLException> {
            b.add(Metric.of("a"), RPoints(LongSlice.wrap(longArrayOf(0)), DoubleSlice.wrap(doubleArrayOf(2.0))))
        }
    }
}
