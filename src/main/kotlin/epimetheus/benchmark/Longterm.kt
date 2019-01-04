package epimetheus.benchmark

import epimetheus.engine.Engine
import epimetheus.engine.PhaseTracer
import epimetheus.model.TimeFrames
import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.storage.Gateway


class Longterm : Workload("long") {
    override fun prepare(gateway: Gateway) {
        // 345600 points per metric
        for (t in 0 until tsDays(30) step 15 * 1000) {
            gateway.pushScraped(t, listOf(
                    ScrapedSample.create("a", 1.0, "x" to "1", "y" to "1"),
                    ScrapedSample.create("a", 1.0, "x" to "1", "y" to "2"),
                    ScrapedSample.create("a", 1.0, "x" to "2", "y" to "1"),
                    ScrapedSample.create("a", 1.0, "x" to "2", "y" to "2"),
                    ScrapedSample.create("b", 1.0, "x" to "1")
            ), false)
        }
    }

    override fun run(engine: Engine): List<BenchmarkResult> {
        val results = mutableListOf<BenchmarkResult>()

        val longTermTimeFrame = TimeFrames(tsDays(0), tsDays(30), tsHours(6))
        val longTermQueries = listOf(
                "a{x=\"1\"} + a{x=\"2\"}",
                "a{x=\"1\",y=\"1\"} + a{x=\"2\",y=\"1\"} + a{x=\"1\",y=\"2\"} + a{x=\"2\",y=\"2\"}",
                "(a{x=\"1\",y=\"1\"} + a{x=\"2\",y=\"1\"}) * (a{x=\"1\",y=\"2\"} + a{x=\"2\",y=\"2\"})",
                "a + on (x) group_left b",
                "rate(a{x=\"1\"}[1d]) + rate(a{x=\"2\"}[1d])",
                "avg_over_time(a[1d])",
                "avg_over_time(a[1h])",
                "max_over_time(a[1d])",
                "histogram_quantile(0.5, a)",
                "histogram_quantile(0.5, rate(a[1h]))",
                "max(rate(a[1d]))",
                "sum(a)",
                "sum by (a) (a)"
        )


        for (query in longTermQueries) {
            val br = benchmark("exec-long:$query") {
                val tracer = PhaseTracer()
                engine.execWithTracer(query, longTermTimeFrame, tracer)
                it["parse"] = (tracer.phases["plan"]!! - tracer.phases["parse"]!!).toDouble() / 1000 / 1000
                it["plan"] = (tracer.phases["exec"]!! - tracer.phases["plan"]!!).toDouble() / 1000 / 1000
                it["exec"] = (tracer.endTime()!! - tracer.phases["exec"]!!).toDouble() / 1000 / 1000
            }
            results += br
        }
        return results
    }
}

