package epimetheus.model

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.fail


class MetricTest {
    @Test
    fun testMetricOf() {
        val mb = MetricBuilder()
        mb.put(Metric.nameLabel, "a")
        mb.put("bar", "baz")
        assertEquals(
                mb.build(),
                Metric.of("a", "bar" to "baz")
        )
    }

    @Test
    fun testFingerprint() {
        val m1 = Metric.of("a", "foo" to "bar")
        val m2 = Metric.of("a", "foo" to "bar")
        val m3 = Metric.of("a", "foo" to "baz")
        assertEquals(m1.fingerprint(), m2.fingerprint())
        assertNotEquals(m1.fingerprint(), m3.fingerprint())
    }

    @Test
    fun testFilter() {
        val m1 = Metric.of("a", "foo" to "bar", "hoge" to "fuga")
        val m2 = Metric.of("a", "hoge" to "fuga")
        assertEquals(m2, m1.filterOn(listOf(Metric.nameLabel, "hoge")))
        assertEquals(m2, m1.filterWithout(false, listOf("foo")))
    }

    @Test
    fun testFingerprintConsistency() {
        val sampleCount = 10000
        val mets = (0 until sampleCount).map { Metric.of("a", "num" to it.toString(), "foo" to "hogehgoe") }.toTypedArray()
        val s1 = mets.sortedBy { it.fingerprint() }
        val s2 = mets.sortedBy { it.fingerprint() }
        assertEquals(s1, s2)
    }

    @Test
    fun testFingerprintUniquenessFNV() {
        val sampleCount = 10000
        val results = LongArray(sampleCount)
        val mets = (0 until sampleCount).map { Metric.of("a", "num" to it.toString(), "foo" to "hogehgoe") }.toTypedArray()
        for (i in 0 until sampleCount) {
            results[i] = mets[i].fingerprint()
        }
        val unieuqnessMap = HashSet<Signature>(sampleCount)
        results.forEachIndexed { index, sig ->
            if (unieuqnessMap.contains(sig)) {
                fail("Uniqueness broken! at index: $index")
            } else {
                unieuqnessMap.add(sig)
            }
        }
    }

    @Test
    fun testFilteredFingerprint() {
        val m1 = Metric.of("a", "foo" to "1", "bar" to "2", "baz" to "3", "hoge" to "4")
        val m2 = Metric.of("a", "foo" to "1", "bar" to "2", "baz" to "a", "hoge" to "c")
        assertEquals(
                m1.filteredFingerprint(true, listOf("foo", "bar")),
                m2.filteredFingerprint(true, listOf("foo", "bar"))
        )
        assertEquals(
                m1.filteredFingerprint(false, listOf("baz", "hoge")),
                m2.filteredFingerprint(false, listOf("baz", "hoge"))
        )
    }
}
