package epimetheus.benchmark

import epimetheus.engine.Engine
import epimetheus.engine.PhaseInterpreterTracer
import epimetheus.engine.SpanTracer
import epimetheus.model.TimeFrames
import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.storage.Gateway

class Wide : Workload("wide") {
    override fun prepare(gateway: Gateway) {
        for (t in 0 until tsMinute(10) step 30 * 1000) {
            gateway.pushScraped(t, (0 until 1000).flatMap {
                listOf(
                        ScrapedSample.create("x", 1.0, "foo" to "m-$it"),
                        ScrapedSample.create("y", 1.0, "foo" to "m-$it")
                )
            }, false)
        }
    }

    override fun run(engine: Engine): List<BenchmarkResult> {
        val results = mutableListOf<BenchmarkResult>()

        val wideTimeFrame = TimeFrames(0, tsMinute(10), tsMinute(1))
        val wideQueries = listOf(
                "rate(x[1m])",
                "rate(x[1m]) + rate(y[1m])",
                "sum(x)",
                "x + y"
        )

        for (query in wideQueries) {
            val br = benchmark("exec-wide:$query") {
                val tracer = SpanTracer()
                engine.execWithTracer(query, wideTimeFrame, tracer)
                it["parse"] = (tracer.phases["plan"]!! - tracer.phases["parse"]!!).toDouble() / 1000 / 1000
                it["plan"] = (tracer.phases["exec"]!! - tracer.phases["plan"]!!).toDouble() / 1000 / 1000
                it["exec"] = (tracer.end!! - tracer.phases["exec"]!!).toDouble() / 1000 / 1000
            }
            results += br
        }
        return results
    }
}
