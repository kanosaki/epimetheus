package epimetheus.engine

import epimetheus.engine.plan.RPointMatrix
import epimetheus.engine.plan.RScalar
import epimetheus.model.Metric
import epimetheus.model.TestUtils.assertValueEquals
import epimetheus.model.TimeFrames
import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.storage.MockGateway
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class EngineTest {
    @Test
    fun testLiteralAndBinOp() {
        val eng = Engine(MockGateway())
        val ts = TimeFrames.instant(0)
        listOf(
                "1" to RScalar(1.0),
                "-1" to RScalar(-1.0),
                "Inf" to RScalar(Double.POSITIVE_INFINITY),
                "1+1" to RScalar(2.0),
                "1*2" to RScalar(2.0),
                "1*-2" to RScalar(-2.0),
                "1/2" to RScalar(0.5),
                "1/(1+1)" to RScalar(0.5)
        ).forEachIndexed { i, pair ->
            val res = eng.exec(pair.first, ts)
            assertEquals(pair.second, res, "$i")
        }
    }

    @Test
    fun evalSelectInstant() {
        data class Param(val query: String, val tf: TimeFrames, val expected: RPointMatrix)

        val cases = listOf(
                Param("a",
                        TimeFrames.instant(0),
                        RPointMatrix.of(listOf(0),
                                Metric.of("a", "b" to "1") to listOf(1.0),
                                Metric.of("a", "b" to "2") to listOf(2.0)
                        )
                ),
                Param("a",
                        TimeFrames.instant(1),
                        RPointMatrix.of(listOf(1),
                                Metric.of("a", "b" to "1") to listOf(3.0),
                                Metric.of("a", "b" to "2") to listOf(4.0)
                        )
                ),
                Param("a",
                        TimeFrames(0, 1, 1),
                        RPointMatrix.of(listOf(0, 1),
                                Metric.of("a", "b" to "1") to listOf(1.0, 3.0),
                                Metric.of("a", "b" to "2") to listOf(2.0, 4.0)
                        )
                ),
                Param("a",
                        TimeFrames(1, 2, 1),
                        RPointMatrix.of(listOf(1, 2),
                                Metric.of("a", "b" to "1") to listOf(3.0, 3.0),
                                Metric.of("a", "b" to "2") to listOf(4.0, 4.0)
                        )
                )
        )
        cases.forEachIndexed { i, c ->
            val mg = MockGateway()
            mg.pushScraped(0, listOf(
                    ScrapedSample.create("a", 1.0, "b" to "1"),
                    ScrapedSample.create("a", 2.0, "b" to "2")
            ))
            mg.pushScraped(1, listOf(
                    ScrapedSample.create("a", 3.0, "b" to "1"),
                    ScrapedSample.create("a", 4.0, "b" to "2")
            ))
            val interp = Engine(mg)
            val mat = interp.exec(c.query, c.tf)
            assertValueEquals(c.expected, mat, msg = "case: $i")
        }
    }

    @Test
    fun evalBasicAggregations() {
        data class Param(val query: String, val tf: TimeFrames, val expected: RPointMatrix)

        val cases = listOf(
                Param("sum(a)",
                        TimeFrames.instant(0),
                        RPointMatrix.of(listOf(0),
                                Metric.empty to listOf(10.0)
                        )
                ),
                Param("sum by (b) (a)",
                        TimeFrames.instant(0),
                        RPointMatrix.of(listOf(0),
                                Metric.of("b" to "1") to listOf(4.0),
                                Metric.of("b" to "2") to listOf(6.0)
                        )
                ),
                Param("sum without (b) (a)",
                        TimeFrames.instant(0),
                        RPointMatrix.of(listOf(0),
                                Metric.of("c" to "1") to listOf(3.0),
                                Metric.of("c" to "2") to listOf(7.0)
                        )
                ),
                Param("avg(a)",
                        TimeFrames.instant(0),
                        RPointMatrix.of(listOf(0),
                                Metric.empty to listOf(10.0 / 4.0)
                        )
                ),
                Param("avg by (b) (a)",
                        TimeFrames.instant(0),
                        RPointMatrix.of(listOf(0),
                                Metric.of("b" to "1") to listOf(4.0 / 2.0),
                                Metric.of("b" to "2") to listOf(6.0 / 2.0)
                        )
                ),
                Param("avg without (b) (a)",
                        TimeFrames.instant(0),
                        RPointMatrix.of(listOf(0),
                                Metric.of("c" to "1") to listOf(3.0 / 2.0),
                                Metric.of("c" to "2") to listOf(7.0 / 2.0)
                        )
                ),
                Param("topk(1, a)",
                        TimeFrames.instant(0),
                        RPointMatrix.of(listOf(0),
                                Metric.of("a", "b" to "2", "c" to "2") to listOf(4.0)
                        )
                )
        )
        cases.forEachIndexed { i, c ->
            val mg = MockGateway()
            mg.pushScraped(0, listOf(
                    ScrapedSample.create("a", 1.0, "b" to "1", "c" to "1"),
                    ScrapedSample.create("a", 2.0, "b" to "2", "c" to "1"),
                    ScrapedSample.create("a", 3.0, "b" to "1", "c" to "2"),
                    ScrapedSample.create("a", 4.0, "b" to "2", "c" to "2")
            ))
            val interp = Engine(mg)
            val mat = interp.exec(c.query, c.tf)
            assertValueEquals(c.expected, mat, msg = "case: $i")
        }
    }

    @Test
    fun evalBasicFunctions() {
        data class Param(val query: String, val tf: TimeFrames, val expected: RPointMatrix)

        val cases = listOf(
                Param("sum_over_time(a{b=\"1\"}[10s])",
                        TimeFrames(10 * 1000, 20 * 1000, 5 * 1000),
                        RPointMatrix.of(listOf(10*1000, 15*1000, 20 * 1000),
                                Metric.of("b" to "1") to listOf(4.0, 3.0, 8.0)
                        )
                )
        )
        cases.forEachIndexed { i, c ->
            val mg = MockGateway()
            mg.pushScraped(0, listOf(
                    ScrapedSample.create("a", 1.0, "b" to "1"),
                    ScrapedSample.create("a", 2.0, "b" to "2")
            ))
            mg.pushScraped(10 * 1000, listOf(
                    ScrapedSample.create("a", 3.0, "b" to "1"),
                    ScrapedSample.create("a", 4.0, "b" to "2")
            ))
            mg.pushScraped(20 * 1000, listOf(
                    ScrapedSample.create("a", 5.0, "b" to "1"),
                    ScrapedSample.create("a", 6.0, "b" to "2")
            ))
            val interp = Engine(mg)
            val mat = interp.exec(c.query, c.tf)
            assertValueEquals(c.expected, mat, msg = "case: $i")
        }
    }
}