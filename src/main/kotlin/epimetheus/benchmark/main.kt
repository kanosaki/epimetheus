package epimetheus.benchmark

import epimetheus.engine.Engine
import epimetheus.model.TimeFrames
import epimetheus.storage.Gateway
import epimetheus.storage.IgniteGateway
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
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

data class Measurement(val beginTs: Long, val durationNano: Long)

data class BenchmarkResult(val name: String, val warmups: List<Measurement>, val results: List<Measurement>) {
    val latencies = results.map { it.durationNano }.sorted()

    fun count(): Int {
        return results.size
    }

    fun latencySum(): Double {
        return latencies.sum().toDouble() / 1000 / 1000
    }

    fun latencyPercentileMs(pct: Double): Double {
        assert(pct in 0.0..1.0)
        val idx = (latencies.size * pct).toInt()
        return if (idx == latencies.size) {
            latencies[idx - 1].toDouble() / 1000 / 1000
        } else {
            latencies[idx].toDouble() / 1000 / 1000
        }
    }
}

inline fun measure(fn: () -> Unit): Measurement {
    val beginTs = System.currentTimeMillis()
    val beginTime = System.nanoTime()
    fn()
    val elapsed = System.nanoTime() - beginTime
    return Measurement(beginTs, elapsed)
}

inline fun benchmark(name: String, fn: () -> Unit): BenchmarkResult {
    println("Benchmark $name")
    val warmupCount = 10
    val warmups = mutableListOf<Measurement>()
    for (i in 0 until warmupCount) {
        warmups += measure {
            fn()
        }
    }
    val measureTimeThresh = 10L * 1000 * 1000 * 1000
    val measureCountThresh = 1000
    var measuredTime = 0L
    val results = mutableListOf<Measurement>()


    while (measuredTime < measureTimeThresh && results.size < measureCountThresh) {
        val m = measure {
            fn()
        }
        measuredTime += m.durationNano
        results += m
    }
    return BenchmarkResult(name, warmups, results)
}

fun main(args: Array<String>) {
    val servers = (0 until 3).map {
        Ignition.start(IgniteConfiguration().apply {
            igniteInstanceName = "server-$it"
        })
    }
    val client = Ignition.start(IgniteConfiguration().apply {
        igniteInstanceName = "client"
        isClientMode = true
    })
    val workloads = listOf(
            Longterm(),
            Wide()
    )
    val gateway = IgniteGateway(client)
    val engine = Engine(gateway, null)

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
                DoubleColumn.create("sum", results.map { it.latencySum() }),
                DoubleColumn.create("50p", results.map { it.latencyPercentileMs(.05) }),
                DoubleColumn.create("90p", results.map { it.latencyPercentileMs(.9) }),
                DoubleColumn.create("99p", results.map { it.latencyPercentileMs(.99) }),
                DoubleColumn.create("max", results.map { it.latencyPercentileMs(1.0) })
        ).sortDescendingOn("99p")
        println(table.printAll())
    }

    servers.forEach { it.close() }
    client.close()
}