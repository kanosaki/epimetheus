package epimetheus.benchmark

import epimetheus.engine.Engine
import epimetheus.model.TimeFrames
import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.storage.IgniteGateway
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.streams.asStream

fun tsDays(n: Int): Long {
    return Duration.of(n.toLong(), ChronoUnit.DAYS).toMillis()
}

fun tsHours(n: Int): Long {
    return Duration.of(n.toLong(), ChronoUnit.HOURS).toMillis()
}

inline fun measureTime(name: String, fn: () -> Unit) {
    val warmupCount = 10
    for (i in 0 until warmupCount) {
        fn()
    }
    val stableNano = 10L * 1000 * 1000 * 1000
    val stableCtr = 100
    var measured = 0L
    val history = mutableListOf<Long>()

    while (measured < stableNano || history.size < stableCtr) {
        val start = System.nanoTime()
        fn()
        val elapsed = System.nanoTime() - start
        history += elapsed
        measured += elapsed
    }
    println("=== $name avg:${history.average() / 1000 / 1000}ms max:${history.max()!! / 1000 / 1000}ms min:${history.min()!! / 1000 / 1000}ms")
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
    val gateway = IgniteGateway(client)
    val engine = Engine(gateway, null)
    val loadStart = System.currentTimeMillis()
    val ctr = AtomicLong(0)
    // 345600 points per metric
    (0 until tsDays(40) step 15 * 1000)
            .asSequence()
            .asStream()
            .parallel().forEach { t ->
                gateway.pushScraped(t, listOf(
                        ScrapedSample.create("a", 1.0, "x" to "1", "y" to "1"),
                        ScrapedSample.create("a", 1.0, "x" to "1", "y" to "2"),
                        ScrapedSample.create("a", 1.0, "x" to "2", "y" to "1"),
                        ScrapedSample.create("a", 1.0, "x" to "2", "y" to "2"),
                        ScrapedSample.create("b", 1.0, "x" to "1", "y" to "1"),
                        ScrapedSample.create("b", 1.0, "x" to "1", "y" to "2"),
                        ScrapedSample.create("b", 1.0, "x" to "2", "y" to "1"),
                        ScrapedSample.create("b", 1.0, "x" to "2", "y" to "2")
                ), false)
                ctr.addAndGet(8)
            }
    gateway.pushScraped(0, listOf(), true)
    println("Load ${ctr.get()}items took ${System.currentTimeMillis() - loadStart}ms")

    val tf = TimeFrames(tsDays(10), tsDays(40), tsHours(6))

    val queries = listOf(
            "avg_over_time(a[1d])",
            "max_over_time(a[1d])",
            "histogram_quantile(0.5, a)",
            "histogram_quantile(0.5, rate(a[1h]))",
            "max(rate(a[1d]))",
            "sum(a)",
            "sum by (a) (a)",
            "a + b"
    )

    for (query in queries) {
        measureTime("exec($query)") {
            engine.exec(query, tf)
        }
    }

    servers.forEach { it.close() }
    client.close()
}