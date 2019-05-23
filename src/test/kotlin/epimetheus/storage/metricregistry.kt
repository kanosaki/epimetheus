package epimetheus.storage

import epimetheus.CacheName
import epimetheus.model.LabelMatchType
import epimetheus.model.LabelMatcher
import epimetheus.model.Metric
import epimetheus.model.MetricMatcher
import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.junit.jupiter.api.*
import kotlin.test.assertEquals


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("slow")
class IgniteMetricRegistryTest {
    lateinit var ignite: Ignite
    @BeforeAll
    fun setUp() {
        ignite = Ignition.getOrStart(IgniteConfiguration())
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
    fun testRegex() {
        val meta = IgniteMeta(ignite)
        meta.registerMetricsFromSamples(listOf(
                ScrapedSample.create("foobar1", 0.0, "foo" to "bar"),
                ScrapedSample.create("foobar2", 0.0, "foo" to "bar", "hoge" to "fuga"),
                ScrapedSample.create("foobar3", 0.0, "foo" to "piyo")
        ))
        val mets = meta.lookupMetrics(
                MetricMatcher(listOf("__name__" to LabelMatcher(LabelMatchType.Match, "foobar1|foobar2")))
        )
        assertEquals(
                setOf(
                        Metric.of("foobar1", "foo" to "bar"),
                        Metric.of("foobar2", "foo" to "bar", "hoge" to "fuga")
                ),
                mets.toSet()
        )
    }
}
