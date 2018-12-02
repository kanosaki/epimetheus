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

    fun test(lm: LabelMatcher, met: Metric, key: String): Boolean {
        val emptyMatching = (lm.lmt == LabelMatchType.Neq || lm.lmt == LabelMatchType.NotMatch || lm.value == "")
        return if (!emptyMatching) {
            val v = met.get(key)
            v != null && lm.matches(v)
        } else {
            val v = met.get(key)
            v == null || lm.matches(v)
        }
    }

    fun matches(met: Metric, ignoreName: Boolean = false): Boolean {
        return matchers.all {
            (ignoreName && it.first == Metric.nameLabel) || // pass name label if ignoreName is true
                    test(it.second, met, it.first)
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

