package epimetheus.model

import org.junit.jupiter.api.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class LabelMatcherTest {
    @Test
    fun testLabelBasicMatch() {
        data class tester(val lm: LabelMatcher, val matches: List<String>, val unmatches: List<String>) {
            fun eval() {
                matches.forEach { assertTrue { lm.matches(it) } }
                unmatches.forEach { assertFalse { lm.matches(it) } }
            }
        }
        listOf(
                tester(LabelMatcher(LabelMatchType.Eq, "foo"), listOf("foo"), listOf("bar", "baz")),
                tester(LabelMatcher(LabelMatchType.Neq, "foo"), listOf("bar", "baz"), listOf("foo")),
                tester(LabelMatcher(LabelMatchType.Match, "foo.*"), listOf("foobar"), listOf("hoge")),
                tester(LabelMatcher(LabelMatchType.Match, "x.*"), listOf("xx", "xy"), listOf("zz")),
                tester(LabelMatcher(LabelMatchType.NotMatch, "foo.*"), listOf("hoge"), listOf("foobar"))
        ).forEach { it.eval() }
    }
}

class MetricMatcherTest {
    @Test
    fun testMetricBasicMatch() {
        val basic = MetricMatcher(mapOf(
                "foo" to LabelMatcher(LabelMatchType.Eq, "bar"),
                "hoge" to LabelMatcher(LabelMatchType.Neq, "fuga")
        ))
        listOf(
                Metric(sortedMapOf("foo" to "bar", "hoge" to "piyo")),
                Metric(sortedMapOf("foo" to "bar", "hoge" to "123", "extra" to "foobar"))
        ).forEach { assertTrue { basic.matches(it) } }

        listOf(
                Metric(sortedMapOf("foo" to "bar")),
                Metric(sortedMapOf("foo" to "bar", "hoge" to "fuga"))
        ).forEach { assertFalse { basic.matches(it) } }

    }
}