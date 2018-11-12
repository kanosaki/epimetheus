package epimetheus.engine

import epimetheus.model.GridMat
import epimetheus.model.Metric
import epimetheus.model.Scalar
import epimetheus.model.TestUtils.assertValueEquals
import epimetheus.model.TimeFrames
import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.storage.MockGateway
import org.junit.jupiter.api.Test

class SimpleTest {
    @Test
    fun evalLiteral() {
        val interp = Interpreter(MockGateway())
        val cases = listOf(
                "1" to 1.0,
                "-1" to -1.0,
                "-1.0" to -1.0,
                "1.3e3" to 1300.0,
                "Inf" to Double.POSITIVE_INFINITY,
                "+Inf" to Double.POSITIVE_INFINITY,
                "-Inf" to Double.NEGATIVE_INFINITY,
                "NaN" to Double.NaN
        )
        val tf = TimeFrames(0, 2, 1)
        cases.forEach {
            val mat = interp.eval(it.first, tf)
            val expected = Scalar(it.second)
            assertValueEquals(expected, mat, true)
        }
    }

    @Test
    fun evalSinglePoint() {
        val mg = MockGateway()
        mg.pushScraped("", 0, listOf(ScrapedSample.create("a", 1.0)))
        mg.pushScraped("", 1, listOf(ScrapedSample.create("a", 1.0)))
        val interp = Interpreter(mg)
        val tf = TimeFrames(0, 1, 1)
        val mat = interp.eval("a", tf)
        assertValueEquals(
                GridMat.of(TimeFrames(0, 1, 1), Metric.of("a") to doubleArrayOf(1.0)),
                mat
        )
    }

    @Test
    fun evalBinOp() {
        val interp = Interpreter(MockGateway())
        val tf = TimeFrames(0, 2, 1)
        listOf(
                "1 + (1 + 2)" to Scalar(4.0),
                "2 + 3 * 2" to Scalar(8.0),
                "3 - 4" to Scalar(-1.0),
                "10 / 5" to Scalar(2.0)
        ).forEach { case ->
            val mat = interp.eval(case.first, tf)
            assertValueEquals(case.second, mat)
        }
    }

    @Test
    fun evalBinOpLiteralAndSelector() {
        val mg = MockGateway()
        mg.pushScraped("", 0, listOf(
                ScrapedSample.create("a", 1.0),
                ScrapedSample.create("b", 3.0)
        ))
        mg.pushScraped("", 1, listOf(
                ScrapedSample.create("a", 2.0),
                ScrapedSample.create("b", 4.0)
        ))
        mg.pushScraped("", 2, listOf(
                ScrapedSample.create("a", 3.0),
                ScrapedSample.create("b", 5.0)
        ))
        val interp = Interpreter(mg)
        val tf = TimeFrames(0, 3, 1)
        listOf(
                "a - 2" to doubleArrayOf(1.0, 0.0, -1.0),
                "2 - a" to doubleArrayOf(-1.0, 0.0, 1.0),
                "a * 2" to doubleArrayOf(6.0, 4.0, 2.0),
                "2 * a" to doubleArrayOf(6.0, 4.0, 2.0),
                "b * a" to doubleArrayOf(15.0, 8.0, 3.0),
                "a + b + 1" to doubleArrayOf(9.0, 7.0, 5.0),
                "a / 0" to doubleArrayOf(Double.NaN, Double.NaN, Double.NaN)
        ).forEach { case ->
            val mat = interp.eval(case.first, tf)
            assertValueEquals(GridMat.of(tf, Metric.empty to case.second), mat, allowNonDetComparsion = true)
        }

    }
}