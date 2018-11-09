package epimetheus.storage

import epimetheus.model.*
import epimetheus.model.TestUtils.assertMatEquals
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.junit.jupiter.api.*
import kotlin.test.assertEquals


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("slow")
class TestStorageIgnite {
    lateinit var ignite: Ignite
    @BeforeAll
    fun setUp() {
        val conf = this.javaClass.getResource("/dev-config.xml")
        ignite = Ignition.start(conf)
    }

    @AfterAll
    fun tearDown() {
        ignite.close()
    }

    @Test
    fun testMetaLookupLucene() {
        val meta = IgniteMeta(ignite)
        meta.registerMetricsFromSamples(listOf(
                ScrapedSample.create("foobar1", 0.0, "foo" to "bar"),
                ScrapedSample.create("foobar2", 0.0, "foo" to "bar", "hoge" to "fuga"),
                ScrapedSample.create("foobar3", 0.0, "foo" to "piyo")
        ))
        val mets = meta.lookupMetrics(
                MetricMatcher
                        .nameMatch("foobar*")
                        .add(
                                "foo" to LabelMatcher(LabelMatchType.Eq, "bar")
                        )
        )
        assertEquals(
                setOf(
                        Metric.of("foobar1", "foo" to "bar"),
                        Metric.of("foobar2", "foo" to "bar", "hoge" to "fuga")
                ),
                mets.toSet()
        )
    }

    @Test
    fun testMetaLookupRegex() {
        val meta = IgniteMeta(ignite)
        meta.registerMetricsFromSamples(listOf(
                ScrapedSample.create("foobar1a", 0.0, "foo" to "bar"),
                ScrapedSample.create("foobar2a", 0.0, "foo" to "bar", "hoge" to "fuga"),
                ScrapedSample.create("foobar3b", 0.0, "foo" to "bar")
        ))
        val mets = meta.lookupMetrics(
                MetricMatcher
                        .nameMatch("foobar?a")
                        .add(
                                "foo" to LabelMatcher(LabelMatchType.Eq, "bar")
                        )
        )
        assertEquals(
                setOf(
                        Metric.of("foobar1a", "foo" to "bar"),
                        Metric.of("foobar2a", "foo" to "bar", "hoge" to "fuga")
                ),
                mets.toSet()
        )
    }

    @Test
    fun testMetaLookupShouldBeSorted() {
        val meta = IgniteMeta(ignite)
        val samplesCount = 10000
        meta.registerMetricsFromSamples((0 until samplesCount).map { ScrapedSample.create("test$it", 0.0) })
        val begin = System.nanoTime()
        val lucene = meta.lookupMetrics(MetricMatcher.nameMatch("test*")).toList()
        val luceneEnd = System.nanoTime()
        val regex = meta.lookupMetrics(MetricMatcher.nameMatch("test.*", true)).toList()
        val regexEnd = System.nanoTime()
        val sorted = lucene.sortedBy { it.fingerprint() }
        assertEquals(lucene.size, samplesCount)
        assertEquals(regex.size, samplesCount)
        assertEquals(sorted.map { it.fingerprint() }, lucene.map { it.fingerprint() })
        assertEquals(sorted.map { it.fingerprint() }, regex.map { it.fingerprint() })
        println("Lookup metrics: Lucene ${(luceneEnd - begin) / 1000 / 1000}ms, Regex ${(regexEnd - luceneEnd) / 1000 / 1000}ms")
    }

    @Test
    fun testPutGetIgnite() {
        TestGateway().execPutGet(IgniteGateway(ignite))
    }
}

class TestGateway {
    @Test
    fun testPutGetMock() {
        execPutGet(MockGateway())
    }

    fun execPutGet(storage: Gateway) {
        for (instance in listOf("a:123", "b:234", "c:345")) {
            for (ts in 0 until 10) {
                val samples = listOf("xx", "xy", "zz")
                        .map { ScrapedSample.create(it, 1.0, "instance" to instance) }
                // record correct data
                storage.pushScraped(instance, ts.toLong(), samples)
            }
        }

        fun met(name: String, instance: String): Metric {
            return Metric(sortedMapOf("__name__" to name, "instance" to instance))
        }

        val actual = storage.collect(MetricMatcher.nameMatch("x.*", true), TimeFrames(0, 3, 1))
        val expected = Mat.of(TimeFrames(0, 3, 1),
                met("xx", "a:123") to doubleArrayOf(1.0, 1.0, 1.0),
                met("xy", "a:123") to doubleArrayOf(1.0, 1.0, 1.0),

                met("xx", "b:234") to doubleArrayOf(1.0, 1.0, 1.0),
                met("xy", "b:234") to doubleArrayOf(1.0, 1.0, 1.0),

                met("xx", "c:345") to doubleArrayOf(1.0, 1.0, 1.0),
                met("xy", "c:345") to doubleArrayOf(1.0, 1.0, 1.0)
        )
        assertMatEquals(expected, actual)
    }
}