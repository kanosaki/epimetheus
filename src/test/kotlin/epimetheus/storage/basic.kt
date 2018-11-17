package epimetheus.storage

import epimetheus.CacheName
import epimetheus.model.*
import epimetheus.model.TestUtils.assertMatEquals
import epimetheus.model.TestUtils.assertRangeMatEquals
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

    @BeforeEach
    fun clearCache() {
        ignite.destroyCache(CacheName.Prometheus.FRESH_SAMPLES)
        ignite.destroyCache(CacheName.Prometheus.METRIC_META)
        ignite.destroyCache(CacheName.Prometheus.SCRAPE_STATUSES)
        ignite.destroyCache(CacheName.Prometheus.SCRAPE_TARGETS)
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
    fun testPutGetInstant() {
        GatewayTest.execTestInstant(IgniteGateway(ignite))
    }

    @Test
    fun testPutGetRange() {
        GatewayTest.execTestRange(IgniteGateway(ignite))
    }

    @Test
    fun testInstantStale() {
        GatewayTest.execTestInstantStale(IgniteGateway(ignite))
    }
}

class TestGateway {
    @Test
    fun testPutGetInstant() {
        GatewayTest.execTestInstant(MockGateway())
    }

    @Test
    fun testPutGetRange() {
        GatewayTest.execTestRange(MockGateway())
    }

    @Test
    fun testInstantStale() {
        GatewayTest.execTestInstantStale(MockGateway())
    }
}

object GatewayTest {
    fun execTestInstant(storage: Gateway) {
        for (instance in listOf("a:123", "b:234", "c:345")) {
            for (ts in 0 until 100 step 10) {
                val samples = listOf("xx", "xy", "zz")
                        .map { ScrapedSample.create(it, ts.toDouble(), "instance" to instance) }
                // record correct data
                storage.pushScraped(instance, ts.toLong(), samples)
            }
        }

        fun met(name: String, instance: String): Metric {
            return Metric(sortedMapOf("__name__" to name, "instance" to instance))
        }

        val actual = storage.collectInstant(MetricMatcher.nameMatch("x.*", true), TimeFrames(5, 25, 10))
        val expected = GridMat.of(TimeFrames(5, 25, 10),
                met("xx", "a:123") to doubleArrayOf(0.0, 10.0, 20.0),
                met("xy", "a:123") to doubleArrayOf(0.0, 10.0, 20.0),
                met("xx", "b:234") to doubleArrayOf(0.0, 10.0, 20.0),
                met("xy", "b:234") to doubleArrayOf(0.0, 10.0, 20.0),
                met("xx", "c:345") to doubleArrayOf(0.0, 10.0, 20.0),
                met("xy", "c:345") to doubleArrayOf(0.0, 10.0, 20.0)
        )
        assertMatEquals(expected, actual)

        val actualOffset = storage.collectInstant(MetricMatcher.nameMatch("x.*", true), TimeFrames(25, 35, 10), 10)
        val expectedOffset = GridMat.of(TimeFrames(25, 35, 10),
                met("xx", "a:123") to doubleArrayOf(10.0, 20.0),
                met("xy", "a:123") to doubleArrayOf(10.0, 20.0),
                met("xx", "b:234") to doubleArrayOf(10.0, 20.0),
                met("xy", "b:234") to doubleArrayOf(10.0, 20.0),
                met("xx", "c:345") to doubleArrayOf(10.0, 20.0),
                met("xy", "c:345") to doubleArrayOf(10.0, 20.0)
        )
        assertMatEquals(expectedOffset, actualOffset)
    }

    /**
     * Tests basic collectRange behavior. It will be collected that all values near by each grid points within specified time window.
     */
    fun execTestRange(storage: Gateway) {
        for (ts in 0..60 step 10) {
            storage.pushScraped("a:123", ts.toLong(), listOf(ScrapedSample.create("xx", ts.toDouble(), "instance" to "a:123")))
        }

        for (ts in 0..60 step 15) {
            storage.pushScraped("a:123", ts.toLong(), listOf(ScrapedSample.create("xy", ts.toDouble(), "instance" to "a:123")))
        }

        fun met(name: String, instance: String): Metric {
            return Metric(sortedMapOf("__name__" to name, "instance" to instance))
        }

        val tf = TimeFrames(20, 60, 20)
        val windowSize = 30L
        val actual = storage.collectRange(MetricMatcher.nameMatch("x.*", true), tf, windowSize)
        val expected = RangeGridMat(listOf(met("xy", "a:123"), met("xx", "a:123")), tf, windowSize,
                listOf(
                        listOf(
                                longArrayOf(0, 15) to doubleArrayOf(0.0, 15.0),
                                longArrayOf(15, 30) to doubleArrayOf(15.0, 30.0),
                                longArrayOf(30, 45, 60) to doubleArrayOf(30.0, 45.0, 60.0)
                        ),
                        listOf(
                                longArrayOf(0, 10, 20) to doubleArrayOf(0.0, 10.0, 20.0),
                                longArrayOf(10, 20, 30, 40) to doubleArrayOf(10.0, 20.0, 30.0, 40.0),
                                longArrayOf(30, 40, 50, 60) to doubleArrayOf(30.0, 40.0, 50.0, 60.0)
                        )
                )
        )
        assertRangeMatEquals(expected, actual)

        val actualOffset = storage.collectRange(MetricMatcher.nameMatch("x.*", true), tf, windowSize, 10)
        val expectedOffset = RangeGridMat(listOf(met("xy", "a:123"), met("xx", "a:123")), tf, windowSize,
                listOf(
                        listOf(
                                longArrayOf(0) to doubleArrayOf(0.0),
                                longArrayOf(0, 15, 30) to doubleArrayOf(0.0, 15.0, 30.0),
                                longArrayOf(30, 45) to doubleArrayOf(30.0, 45.0)
                        ),
                        listOf(
                                longArrayOf(0, 10) to doubleArrayOf(0.0, 10.0),
                                longArrayOf(0, 10, 20, 30) to doubleArrayOf(0.0, 10.0, 20.0, 30.0),
                                longArrayOf(20, 30, 40, 50) to doubleArrayOf(20.0, 30.0, 40.0, 50.0)
                        )
                )
        )
        assertRangeMatEquals(expectedOffset, actualOffset)
    }

    /**
     * At Gateway.collectInstant, values near by each grid points within 5 minutes will adopted.
     */
    fun execTestInstantStale(storage: Gateway) {
        storage.pushScraped("a:123", 0 * 60 * 1000, listOf(ScrapedSample.create("xx", 1.0, "instance" to "a:123")))
        storage.pushScraped("a:123", 1 * 60 * 1000, listOf(ScrapedSample.create("xy", 1.0, "instance" to "a:123")))

        fun met(name: String, instance: String): Metric {
            return Metric(sortedMapOf("__name__" to name, "instance" to instance))
        }

        listOf(
                TimeFrames.instant(1 * 60 * 1000) to
                        arrayOf(
                                met("xx", "a:123") to doubleArrayOf(1.0),
                                met("xy", "a:123") to doubleArrayOf(1.0)
                        ),
                TimeFrames.instant(5 * 60 * 1000) to
                        arrayOf(
                                met("xx", "a:123") to doubleArrayOf(1.0),
                                met("xy", "a:123") to doubleArrayOf(1.0)
                        ),
                TimeFrames.instant(5 * 60 * 1000 + 1) to
                        arrayOf(
                                met("xy", "a:123") to doubleArrayOf(1.0)
                        ),
                TimeFrames.instant(6 * 60 * 1000 + 1) to
                        arrayOf()
        ).forEachIndexed { index, pair ->
            val actual = storage.collectInstant(MetricMatcher.nameMatch("x.*", true), pair.first).prune()
            val expected = GridMat.of(pair.first, *pair.second)
            assertMatEquals(expected, actual, memo = "at index = $index")
        }
    }

}

