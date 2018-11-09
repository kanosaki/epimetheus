package epimetheus.engine

import epimetheus.pkg.promql.NumberLiteral
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals

// A test that tests test

class PromSpecTest {
    @Test
    fun testLoadCmd() {
        val ps = PromSpec(listOf(
                "load 5m",
                "foo{bar=\"foo\"} 0+1x5 10+2x10 1x10",
                "{} _ x5 stale x10",
                "x{y=\"testvalue\"} 0+10x10"
        ))
        assertEquals(listOf(
                PromSpec.LoadCmd(Duration.ofMinutes(5), listOf(
                        SpecSeriesDesc("foo", arrayOf("bar" to "foo"), listOf(
                                VNumber(0.0, 1.0, 5),
                                VNumber(10.0, 2.0, 10),
                                VNumber(1.0, 0.0, 10)
                        )),
                        SpecSeriesDesc("", arrayOf(), listOf(
                                VBlank(5),
                                VStale(10)
                        )),
                        SpecSeriesDesc("x", arrayOf("y" to "testvalue"), listOf(
                                VNumber(0.0, 10.0, 10)
                        ))
                ))),
                ps.commands
        )
    }

    @Test
    fun testSpecSeriesDesc() {
        SpecSeriesDesc("", arrayOf(), listOf())
    }

    @Test
    fun testEvalCmd() {
        val cases = listOf(
                "eval instant at 5m 1" to PromSpec.EvalCmd(NumberLiteral(1.0), Duration.ofMinutes(5), false, false, listOf()),
                "eval_ordered instant at 5m 1" to PromSpec.EvalCmd(NumberLiteral(1.0), Duration.ofMinutes(5), true, false, listOf()),
                "eval_fail instant at 5m 1" to PromSpec.EvalCmd(NumberLiteral(1.0), Duration.ofMinutes(5), false, true, listOf()),
                "eval instant 1" to PromSpec.EvalCmd(NumberLiteral(1.0), null, false, false, listOf()),
                "eval_ordered instant 1" to PromSpec.EvalCmd(NumberLiteral(1.0), null, true, false, listOf()),
                "eval_fail instant 1" to PromSpec.EvalCmd(NumberLiteral(1.0), null, false, true, listOf())
        )
        cases.forEach {
            assertEquals(listOf(it.second), PromSpec(listOf(it.first)).commands, "error at '${it.first}'")
        }
    }
}