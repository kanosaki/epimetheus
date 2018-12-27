package epimetheus.model

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith


class DoubleSlcieTest {
    @Test
    fun testListInterface() {
        val origin = (0..10).map { it.toDouble() }.toDoubleArray()
        val ds1 = DoubleSlice(origin, 2, 2)
        val ds2 = DoubleSlice(origin, 3, 3)
        assertEquals(listOf(2.0, 3.0), ds1.toList())
        assertEquals(listOf(3.0, 4.0, 5.0), ds2.toList())

        assertEquals(2.0, ds1[0])
        assertEquals(3.0, ds1[1])
        assertFailsWith(IndexOutOfBoundsException::class) {
            ds1[2]
        }

        // two slice shares backed array, so side-effects will be propagated
        ds1.write(1, 0.0)
        assertEquals(listOf(2.0, 0.0), ds1.toList())
        assertEquals(listOf(0.0, 4.0, 5.0), ds2.toList())
    }

    @Test
    fun testMapCopy() {
        val origin = (0..10).map { it.toDouble() }.toDoubleArray()
        val ds1 = DoubleSlice(origin, 2, 2)
        val ds2 = ds1.mapCopy { _, d -> d + 1 }
        assertEquals(listOf(2.0, 3.0), ds1.toList())
        assertEquals(listOf(3.0, 4.0), ds2.toList())

        // mapCopy copes source array, so changes does not affect original slice
        ds2.values[0] = 0.0
        assertEquals(listOf(2.0, 3.0), ds1.toList())
        assertEquals(listOf(0.0, 4.0), ds2.toList())
    }
}

class LongSliceTest {
    @Test
    fun testListInterface() {
        val origin = (0L..10).toList().toLongArray()
        val ds1 = LongSlice(origin, 2, 2)
        val ds2 = LongSlice(origin, 4, 3)
        assertEquals(listOf(2L, 3), ds1.toList())
        assertEquals(listOf(4L, 5, 6), ds2.toList())
        assertEquals(2L, ds1[0])
        assertEquals(3L, ds1[1])
        assertFailsWith(IndexOutOfBoundsException::class) {
            ds1[2]
        }
    }
}