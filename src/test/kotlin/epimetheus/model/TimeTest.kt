package epimetheus.model

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TimeFramesTest {
    @Test
    fun testListInterface() {
        val tf = TimeFrames(0, 10, 2)
        assertEquals(tf.size, 6)
        assertEquals(0, tf[0])
        assertEquals(4, tf[2])
        assertEquals(8, tf[4])
        assertEquals(10, tf[5])
        assertEquals(true, tf.contains(2))
        assertEquals(false, tf.contains(3))
        assertEquals(0, tf.indexOf(0))
        assertEquals(3, tf.indexOf(6))
        assertEquals(-1, tf.indexOf(1))
        assertEquals(false, tf.isEmpty())
        assertEquals(listOf<Long>(0, 2, 4, 6, 8, 10), tf.toList())

        assertEquals(listOf<Long>(2, 4, 6, 8), tf.subList(1, tf.size - 2).toList())

    }

    @Test
    fun testRangeCheck() {
        val tf = TimeFrames(0, 10, 2)
        assertFalse(tf.includes(-1))
        assertTrue(tf.includes(0))
        assertTrue(tf.includes(1))
        assertTrue(tf.includes(7))
        assertTrue(tf.includes(8))
        assertTrue(tf.includes(9))
        assertTrue(tf.includes(10))
        assertFalse(tf.includes(11))
        assertFalse(tf.includes(12))

        assertFalse(tf.contains(-1))
        assertTrue(tf.contains(0))
        assertFalse(tf.contains(1))
        assertFalse(tf.contains(7))
        assertTrue(tf.contains(8))
        assertFalse(tf.contains(9))
        assertTrue(tf.contains(10))
        assertFalse(tf.contains(11))
        assertFalse(tf.contains(12))
    }

    @Test
    fun testStretchStart() {
        val tf = TimeFrames(4, 6, 2)
        assertEquals(tf.stretchStart(0, true), TimeFrames(4, 6, 2))
        assertEquals(tf.stretchStart(0, false), TimeFrames(4, 6, 2))
        assertEquals(tf.stretchStart(3, true), TimeFrames(0, 6, 2))
        assertEquals(tf.stretchStart(3, false), TimeFrames(2, 6, 2))
    }

    @Test
    fun testIterateTwice() {
        val tf = TimeFrames(0, 24, 3)
        assertEquals(tf.toList(), tf.toList())
    }
}