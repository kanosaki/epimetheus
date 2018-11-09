package epimetheus.model

enum class LabelMatchType {
    Eq,
    Neq,
    Match,
    NotMatch,
}

data class LabelMatcher(val lmt: LabelMatchType, val value: String) {
    private val pat = if (lmt == LabelMatchType.Match || lmt == LabelMatchType.NotMatch) Regex(value) else null

    fun matches(labelValue: String): Boolean {
        return when (lmt) {
            LabelMatchType.Eq -> labelValue == value
            LabelMatchType.Neq -> labelValue != value
            LabelMatchType.Match -> pat!!.matches(labelValue)
            LabelMatchType.NotMatch -> !pat!!.matches(labelValue)
        }
    }
}

data class MetricMatcher(val matchers: Map<String, LabelMatcher>) {
    fun namePattern(): LabelMatcher? {
        return matchers[Metric.nameLabel]
    }

    fun matches(met: Metric, ignoreName: Boolean = false): Boolean {
        val m = met.m
        return matchers.all {
            (ignoreName && it.key == Metric.nameLabel) || // pass name label if ignoreName is true
                    m.containsKey(it.key) && it.value.matches(m[it.key]!!)
        }
    }

    fun add(vararg patterns: Pair<String, LabelMatcher>): MetricMatcher {
        return MetricMatcher(matchers.plus(patterns))
    }

    companion object {
        fun nameMatch(name: String, regex: Boolean = false): MetricMatcher {
            val lmt = if (regex) LabelMatchType.Match else LabelMatchType.Eq
            return MetricMatcher(mapOf(Metric.nameLabel to LabelMatcher(lmt, name)))
        }
    }
}

