package epimetheus.benchmark

import epimetheus.engine.Engine
import epimetheus.storage.Gateway
import epimetheus.storage.IgniteGateway
import tech.tablesaw.api.DoubleColumn
import tech.tablesaw.api.StringColumn
import tech.tablesaw.api.Table
import java.time.Duration
import java.time.temporal.ChronoUnit

fun tsDays(n: Int): Long {
    return Duration.of(n.toLong(), ChronoUnit.DAYS).toMillis()
}

fun tsHours(n: Int): Long {
    return Duration.of(n.toLong(), ChronoUnit.HOURS).toMillis()
}

fun tsMinute(n: Int): Long {
    return Duration.of(n.toLong(), ChronoUnit.MINUTES).toMillis()
}

abstract class Workload(val name: String) {
    abstract fun prepare(gateway: Gateway)
    abstract fun run(engine: Engine): List<BenchmarkResult>
}

data class Measurement(val beginTs: Long, val wholeLatency: Long, val partLatencies: Map<String, Long> = mapOf())

data class BenchmarkResult(val name: String, val results: LatencyMap) {
    val latencies = listOf("whole", "parse", "plan", "exec").map { it to results.col(it).sorted() }.toMap()

    fun count(): Int {
        return results.size
    }

    fun latencySum(): Double {
        return latencies["whole"]!!.sum()
    }

    fun latencyPercentileMs(pct: Double): Double {
        return latencyPercentileMs("whole", pct)
    }

    fun latencyPercentileMs(col: String, pct: Double): Double {
        assert(pct in 0.0..1.0)
        val idx = (latencies.size * pct).toInt()
        return if (idx == latencies.size) {
            latencies[col]!![idx - 1]
        } else {
            latencies[col]!![idx]
        }
    }
}

inline fun measure(w: LatencyMap.Writer, fn: () -> Unit): Long {
    val beginTime = System.nanoTime()
    fn()
    val elapsed = System.nanoTime() - beginTime
    w["whole"] = elapsed.toDouble() / 1000 / 1000
    return elapsed
}

inline fun benchmark(name: String, fn: (LatencyMap.Writer) -> Unit): BenchmarkResult {
    println("Benchmark $name")
    val warmupCount = 10
    val voidWriter = LatencyMap.Writer.Void
    for (i in 0 until warmupCount) {
        measure(voidWriter) {
            fn(voidWriter)
        }
    }
    val measureTimeThresh = 10L * 1000 * 1000 * 1000 // 10 seconds
    val measureCountThresh = 1000
    var measuredTime = 0L
    val results = mutableListOf<Measurement>()
    val lm = LatencyMap(listOf("begin", "whole", "parse", "plan", "exec"))

    while (measuredTime < measureTimeThresh && results.size < measureCountThresh) {
        val w = lm.newWriter()
        w["begin"] = System.currentTimeMillis().toDouble()
        val elapsed = measure(w) {
            fn(w)
        }
        measuredTime += elapsed
        w.commit()
    }
    return BenchmarkResult(name, lm)
}

fun main(args: Array<String>) {
    val cluster = ClusterManager(3)
    val client = cluster.start()

    val workloads = listOf(
            Longterm(),
            Wide()
    )
    val gateway = IgniteGateway(client)
    val engine = Engine(gateway, null)

    try {
        val loadStart = System.currentTimeMillis()

        for (w in workloads) {
            println("Prepare ${w.name}")
            w.prepare(gateway)
        }

        gateway.pushScraped(0, listOf(), true)
        println("Prepare took ${System.currentTimeMillis() - loadStart}ms")


        for (w in workloads) {
            println("Run ${w.name}")
            val results = w.run(engine)

            val table = Table.create(w.name,
                    StringColumn.create("name", results.map { it.name }),
                    DoubleColumn.create("count", results.map { it.count() }),
                    DoubleColumn.create("50p", results.map { it.latencyPercentileMs(.05) }),
                    DoubleColumn.create("90p", results.map { it.latencyPercentileMs(.9) }),
                    DoubleColumn.create("99p", results.map { it.latencyPercentileMs(.99) }),
                    DoubleColumn.create("parse-50p", results.map { it.latencyPercentileMs("parse", .5) }),
                    DoubleColumn.create("plan-50p", results.map { it.latencyPercentileMs("plan", .5) }),
                    DoubleColumn.create("exec-50p", results.map { it.latencyPercentileMs("exec", .5) }),
                    DoubleColumn.create("duration", results.map { it.latencySum() })
            ).sortDescendingOn("99p")
            println(table.printAll())
        }
    } finally {
        cluster.stop()
    }
}