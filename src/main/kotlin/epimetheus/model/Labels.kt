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

data class MetricMatcher(val matchers: List<Pair<String, LabelMatcher>>) {
    private val namePat = matchers.firstOrNull { it.first == Metric.nameLabel }

    fun namePattern(): LabelMatcher? {
        return namePat?.second
    }

    fun matches(met: Metric, ignoreName: Boolean = false): Boolean {
        val m = met.m
        return matchers.all {
            (ignoreName && it.first == Metric.nameLabel) || // pass name label if ignoreName is true
                    m.containsKey(it.first) && it.second.matches(m[it.first]!!)
        }
    }

    fun add(vararg patterns: Pair<String, LabelMatcher>): MetricMatcher {
        return MetricMatcher(matchers.plus(patterns))
    }

    companion object {
        fun nameMatch(name: String, regex: Boolean = false): MetricMatcher {
            val lmt = if (regex) LabelMatchType.Match else LabelMatchType.Eq
            return MetricMatcher(listOf(Metric.nameLabel to LabelMatcher(lmt, name)))
        }
    }
}

