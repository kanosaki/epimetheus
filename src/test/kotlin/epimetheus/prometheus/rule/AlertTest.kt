package epimetheus.prometheus.rule

import epimetheus.job.JobService
import epimetheus.model.Metric
import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.storage.IgniteGateway
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.services.ServiceConfiguration
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("slow")
class AlertTest {

    private val serviceName = "test_job_service"
    private val ruleGroup = "test"
    private val pollInterval = 100L
    private lateinit var ignite: Ignite

    @BeforeAll
    fun setUp() {
        ignite = Ignition.getOrStart(IgniteConfiguration())
        ignite.services().deploy(ServiceConfiguration().apply {
            name = serviceName
            service = JobService(pollInterval)
            maxPerNodeCount = 1
        })
    }

    @AfterAll
    fun tearDown() {
        ignite.services().cancel(serviceName)
    }

    @AfterEach
    fun clearRules() {
        val g = RuleGateway(ignite)
        g.clear(ruleGroup)
    }

    @Test
    fun testAlertBasicFire() {
        val g = RuleGateway(ignite)
        g.putRuleGroup(ruleGroup, Duration.ofSeconds(1))
        val rule = AlertingRule(ruleGroup, "test_alert", "1")
        g.putRule(ruleGroup, rule)
        Thread.sleep(800)
        val status = g.getAlertStatus(RuleKey(ruleGroup, rule.id))
        assertTrue(status != null)
        assertTrue(status!!.firedSince[Metric.empty] != null)
    }

    @Test
    fun testAlertBasicNonFire() {
        val g = RuleGateway(ignite)
        g.putRuleGroup(ruleGroup, Duration.ofSeconds(1))
        val rule = AlertingRule(ruleGroup, "test_alert_nonfire", "0")
        g.putRule(ruleGroup, rule)
        Thread.sleep(800)
        val status = g.getAlertStatus(RuleKey(ruleGroup, rule.id))
        assertTrue(status != null)
        assertTrue(status!!.firedSince[Metric.empty] == null)
    }

    @Test
    fun testMultiMetricsWithPending() {
        val g = RuleGateway(ignite)
        val storage = IgniteGateway(ignite)
        storage.pushScraped(
                System.currentTimeMillis(),
                listOf(
                        ScrapedSample.create("foo", 1.0, "a" to "1"),
                        ScrapedSample.create("foo", 2.0, "a" to "2"),
                        ScrapedSample.create("foo", 3.0, "a" to "3"),
                        ScrapedSample.create("foo", 4.0, "a" to "4")
                ), true)
        g.putRuleGroup(ruleGroup, Duration.ofSeconds(1))
        val rule1 = AlertingRule(ruleGroup, "test_alert_multi1", "foo > 2")
        val rule2 = AlertingRule(ruleGroup, "test_alert_multi2", "foo > 2", waitForMilliseconds = 1000)
        g.putRule(ruleGroup, rule1)
        g.putRule(ruleGroup, rule2)

        Thread.sleep(500)

        val status11 = g.getAlertStatus(RuleKey(ruleGroup, rule1.id))
        assertEquals(
                mapOf(
                        Metric.of("foo", "a" to "3") to AlertStatus.FIRING,
                        Metric.of("foo", "a" to "4") to AlertStatus.FIRING
                ),
                status11?.alertAndPendings(0)?.toMap()
        )

        val status12 = g.getAlertStatus(RuleKey(ruleGroup, rule2.id))
        assertEquals(
                mapOf(
                        Metric.of("foo", "a" to "3") to AlertStatus.PENDING,
                        Metric.of("foo", "a" to "4") to AlertStatus.PENDING
                ),
                status12?.alertAndPendings(1000)?.toMap()
        )

        Thread.sleep(2000)

        val status22 = g.getAlertStatus(RuleKey(ruleGroup, rule2.id))
        assertEquals(
                mapOf(
                        Metric.of("foo", "a" to "3") to AlertStatus.FIRING,
                        Metric.of("foo", "a" to "4") to AlertStatus.FIRING
                ),
                status22?.alertAndPendings(1000)?.toMap()
        )
    }
}
