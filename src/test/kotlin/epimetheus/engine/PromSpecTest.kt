package epimetheus.engine

import epimetheus.model.Mat
import epimetheus.model.Metric
import epimetheus.pkg.promql.NumberLiteral
import epimetheus.storage.MockGateway
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// A test that tests test

class PromSpecTest {
    @Test
    fun testStaleExpand() {
        assertTrue(Mat.isStale(VStale(0).expand(null).first()!!))
    }

    @Test
    fun testLoadCmdExpand() {
        val ctx = SpecContext(EvaluatorEngine()) { MockGateway() }
        val ps = PromSpec(listOf(
                "load 5m",
                "foo{bar=\"foo\"} 0+1x5 10+2x10 1x10",
                "{} _ x5 stale x10",
                "x{y=\"testvalue\"} 0+10x10"
        ), ctx)
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
    fun testLoad() {
        val gw = MockGateway()
        val ctx = SpecContext(EvaluatorEngine()) { gw }
        val ps = PromSpec(listOf(
                "load 5m",
                "a 0+1x3 10+2x3 1x3",
                "b _ x5 stale x10",
                "c 0+10x5"
        ), ctx)
        ps.execute()

        class AssertCase(
                val metric: String,
                val values: List<Double>
        )

        val cases = listOf(
                AssertCase("a", listOf(0.0, 1.0, 2.0, 3.0, 10.0, 12.0, 14.0, 16.0, 1.0, 1.0, 1.0, 1.0)),
                AssertCase("c", listOf(0.0, 10.0, 20.0, 30.0, 40.0))
        )
        cases.forEach { case ->
            val met = Metric.of(case.metric)
            assertEquals(
                    case.values,
                    gw.datum[met.fingerprint()]!!.values.toList()
            )
            val minute = 60L * 1000
            assertEquals(
                    (LongArray(case.values.size) { it * 5L * minute }).toList(),
                    gw.datum[met.fingerprint()]!!.keys.toList()
            )
        }

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
        val ctx = SpecContext(EvaluatorEngine()) { MockGateway() }
        cases.forEach {
            assertEquals(listOf(it.second), PromSpec(listOf(it.first), ctx).commands, "error at '${it.first}'")
        }
    }
}
