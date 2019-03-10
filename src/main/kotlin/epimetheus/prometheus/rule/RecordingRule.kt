package epimetheus.prometheus.rule

class RecordingRule(
        override val group: String,
        val record: String,
        val expr: String,
        val labels: Map<String, String> = mapOf()) : Rule {
    override val id: String
        get() = record

}
