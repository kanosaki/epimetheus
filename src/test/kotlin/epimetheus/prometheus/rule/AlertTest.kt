package epimetheus.prometheus.rule

import epimetheus.job.JobService
import epimetheus.model.Metric
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.services.ServiceConfiguration
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertTrue
import java.time.Duration
import kotlin.test.assertEquals

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
}
