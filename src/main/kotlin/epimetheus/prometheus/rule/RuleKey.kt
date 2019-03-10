package epimetheus.prometheus.rule

import epimetheus.job.JobKey

data class RuleKey(val group: String, val id: String): JobKey
