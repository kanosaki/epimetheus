package epimetheus.prometheus.rule

class AlertingRule(
        override val group: String,
        val alert: String,
        val expr: String,
        val waitForMilliseconds: Long = 0,
        val labels: Map<String, String> = mapOf(),
        val annotations: Map<String, String> = mapOf()) : Rule {

    override val id: String
        get() = alert
}
