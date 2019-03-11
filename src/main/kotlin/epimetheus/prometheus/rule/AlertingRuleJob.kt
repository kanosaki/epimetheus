package epimetheus.prometheus.rule

import epimetheus.engine.Engine
import epimetheus.engine.plan.RPointMatrix
import epimetheus.engine.plan.RScalar
import epimetheus.job.JobExitStatus
import epimetheus.job.JobRunnable
import epimetheus.model.Metric
import epimetheus.model.TimeFrames
import epimetheus.storage.IgniteGateway
import org.apache.ignite.Ignite

class AlertingRuleJob(val key: RuleKey, val rule: AlertingRule) : JobRunnable {

    override fun call(ignite: Ignite): JobExitStatus {
        val gateway = IgniteGateway(ignite)
        val ruleGateway = RuleGateway(ignite)
        val engine = Engine(gateway, null)
        val now = System.currentTimeMillis()
        val v = engine.exec(rule.expr, TimeFrames.instant(now))
        val alertingMetrics = mutableListOf<Metric>()
        when (v) {
            is RScalar -> {
                if (v.value >= 1.0) {
                    alertingMetrics.add(Metric.empty)
                }
            }
            is RPointMatrix -> {
                for (i in 0 until v.rowCount) {
                    val series = v.series[i]
                    val metric = v.metrics[i]
                    if (series.values.size > 0) {
                        if (series.values[0] >= 1.0) {
                            alertingMetrics.add(metric)
                        }
                    }
                }
            }
        }

        val status = ruleGateway.getAlertStatus(key)
        val prevStatus = status?.firedSince
        val nextStatus = mutableMapOf<Metric, Long>()
        val firedMetrics = mutableListOf<Metric>()

        for (met in alertingMetrics) {
            val firedSince = prevStatus?.get(met)
            if (firedSince != null && now - firedSince > rule.waitForMilliseconds) {
                firedMetrics.add(met)
            }
            if (firedSince == null) {
                nextStatus[met] = now
            } else {
                nextStatus[met] = firedSince
            }
        }

        // update alert status
        ruleGateway.putAlertStatus(key, AlertEvalResult(nextStatus, now))

        if (firedMetrics.isNotEmpty()) {
            // fire alerts
        }
        return JobExitStatus.Done
    }
}
