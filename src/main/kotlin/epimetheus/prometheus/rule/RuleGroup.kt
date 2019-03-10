package epimetheus.prometheus.rule

class RuleGroup(
        val name: String,
        // default:: global.evaluation_interval
        val intervalMilliseconds: Long)
