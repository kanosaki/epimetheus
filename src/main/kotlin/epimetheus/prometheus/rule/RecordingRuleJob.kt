package epimetheus.prometheus.rule

import epimetheus.job.JobExitStatus
import epimetheus.job.JobRunnable
import org.apache.ignite.Ignite

class RecordingRuleJob(val rule: RecordingRule) : JobRunnable {
    override fun call(ignite: Ignite): JobExitStatus {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
