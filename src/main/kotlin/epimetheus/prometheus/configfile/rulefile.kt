package epimetheus.prometheus.configfile

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Duration

data class RuleFile(
        @JsonProperty("groups")
        val groups: List<RuleGroup>
)

data class RuleGroup(
        @JsonProperty("rules")
        val rules: List<RuleItem>,
        @JsonProperty("interval")
        val interval: Duration?
)


interface RuleItem

data class AlertRule(
        @JsonProperty("alert", required = true)
        val name: String,

        @JsonProperty("expr")
        val expr: String,

        @JsonProperty("for")
        val threshold: Duration,

        @JsonProperty("labels")
        val labels: Map<String, String>,

        @JsonProperty("annotations")
        val annotations: Map<String, String>
) : RuleItem

data class RecordingRule(
        @JsonProperty("record")
        val name: String,

        @JsonProperty("expr")
        val expr: String,

        @JsonProperty("labels")
        val labels: Map<String, String>
) : RuleItem