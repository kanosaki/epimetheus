package epimetheus.prometheus.rule

import epimetheus.CacheName
import epimetheus.ClusterConfig
import epimetheus.EpimetheusException
import epimetheus.job.JobGateway
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.configuration.CacheConfiguration
import java.time.Duration

class RuleGateway(val ignite: Ignite) {
    private val config = ClusterConfig(ignite)
    private val ruleGroupConf = CacheConfiguration<String, RuleGroup>().apply {
        name = CacheName.Prometheus.RULE_GROUP
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL
        backups = 1
    }

    private val ruleConf = CacheConfiguration<RuleKey, Rule>().apply {
        name = CacheName.Prometheus.RULE_CONF
        backups = 1
    }

    private val alertStatusConf = CacheConfiguration<RuleKey, AlertEvalResult>().apply {
        name = CacheName.Prometheus.ALERT_STATUS
        backups = 1
    }

    private val ruleCache: IgniteCache<RuleKey, Rule>
    private val ruleGroupCache: IgniteCache<String, RuleGroup>
    private val alertEvalResultCache: IgniteCache<RuleKey, AlertEvalResult>
    private val jobGateway: JobGateway

    init {
        ruleCache = ignite.getOrCreateCache(ruleConf)
        ruleGroupCache = ignite.getOrCreateCache(ruleGroupConf)
        alertEvalResultCache = ignite.getOrCreateCache(alertStatusConf)

        jobGateway = JobGateway(ignite)
    }

    fun putRuleGroup(name: String, interval: Duration? = null) {
        val intervalMsec = interval?.toMillis() ?: config.prometheusGlobal.scrapeInterval.toMillis()
        lockGroup(name) {
            ruleGroupCache.put(name, RuleGroup(name, intervalMsec))
        }
    }

    fun removeGroup(name: String) {
        // TODO: remove all child rule and remove group its self
        TODO("not implemented")
    }

    fun putRule(group: String, rule: Rule) {
        val key = RuleKey(group, rule.id)
        lockGroup(group) {
            val grp = ruleGroupCache.get(group) ?: throw EpimetheusException("RuleGroup $group not found")
            ruleCache.put(key, rule)
            val r = when (rule) {
                is AlertingRule -> AlertingRuleJob(key, rule)
                is RecordingRule -> RecordingRuleJob(rule)
                else -> throw EpimetheusException("Job for $rule is not defined")
            }
            jobGateway.scheduleInterval(key, r, Duration.ofMillis(grp.intervalMilliseconds))
        }
    }

    /**
     * For testing
     */
    fun clear(group: String) {
        val rules = ruleCache.filter { it.key.group == group }.toList()
        rules.forEach {
            jobGateway.remove(it.key)
        }
        val rulesSet = rules.map { it.key }.toSet()
        ruleCache.clearAll(rulesSet)
        alertEvalResultCache.clearAll(rulesSet)
    }

    fun getAlertStatus(key: RuleKey): AlertEvalResult? {
        return alertEvalResultCache.get(key)
    }

    fun putAlertStatus(key: RuleKey, evalResult: AlertEvalResult) {
        alertEvalResultCache.put(key, evalResult)
    }

    private inline fun lockGroup(name: String, fn: () -> Unit) {
        val lck = ruleGroupCache.lock(name)
        lck.lock()
        try {
            fn()
        } finally {
            lck.unlock()
        }
    }
}
