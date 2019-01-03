package epimetheus.prometheus.scrape

import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.storage.IgniteGateway
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
import org.apache.ignite.Ignite
import org.apache.ignite.lang.IgniteRunnable
import org.apache.ignite.resources.IgniteInstanceResource
import org.apache.ignite.services.Service
import org.apache.ignite.services.ServiceContext
import java.io.Closeable
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ScrapeService : Service {
    companion object {
        val ScanRangeMilliseconds = 10 * 1000L
    }

    @IgniteInstanceResource
    lateinit var ignite: Ignite
    lateinit var vertx: Vertx
    lateinit var client: WebClient
    lateinit var storage: IgniteGateway
    lateinit var scrapeGate: ScrapeGateway

    lateinit var submitThread: ExecutorService


    override fun init(ctx: ServiceContext?) {
        submitThread = Executors.newScheduledThreadPool(1)
        vertx = Vertx.vertx()
        client = WebClient.create(vertx)
        storage = IgniteGateway(ignite)
        scrapeGate = ScrapeGateway(ignite)
    }

    override fun cancel(ctx: ServiceContext?) {
    }

    override fun execute(ctx: ServiceContext?) {
        while (!ctx!!.isCancelled) {
            val now = LocalDateTime.now()
            scrapeGate.localStatuses().map { kv ->
                schedule(now, kv.key, kv.value)
            }
            Thread.sleep(ScanRangeMilliseconds)
        }
    }

    private fun updateTargetStatus(target: ScrapeTargetKey, succeed: Boolean) {
        storage.pushScraped(System.currentTimeMillis(), listOf(
                ScrapedSample.create("up", if (succeed) 1.0 else 0.0, "instance" to target.target)),
                false) // TODO: false?
    }

    private fun writeSamples(target: ScrapeTargetKey, cfg: ScrapeTarget, results: List<ScrapedSample>) {
        storage.pushScraped(System.currentTimeMillis(),
                results.map {
                    val mb = it.met.builder()
                    if (!cfg.honorLabels) {
                        mb.put("instance", target.target)
                        mb.put("job", target.jobName)
                    }
                    ScrapedSample(mb.build(), it.value)
                },
                false) // TODO: false?
    }


    fun doScrape(key: ScrapeTargetKey) {
        val cfg = scrapeGate.target(key)
        val scr = Scraper(client, cfg)
        vertx.executeBlocking<ScrapeResult>(scr, false, Handler { ar ->
            val finishedAt = LocalDateTime.now()
            val status = when (ar.succeeded()) {
                true -> {
                    val samples = ar.result().samples
                    println("SCRAPED ${cfg.url} ${samples.size} samples")
                    // process background to avoid blocking vert.x event loop thread
                    submitThread.submit {
                        writeSamples(key, cfg, samples)
                        updateTargetStatus(key, true)
                    }
                    ScrapeStatusSuccess(ar.result().latencyNs)
                }
                false -> {
                    println("SCRAPE FAILED ${cfg.url} ${ar.cause()}")
                    submitThread.submit {
                        updateTargetStatus(key, false)
                    }
                    ScrapeStatusFailure(ar.cause())
                }
            }
            val sched = ScrapeSchedule(
                    finishedAt.plusNanos((cfg.intervalSeconds * 1e9).toLong()),
                    finishedAt,
                    status
            )
            scrapeGate.updateStatus(key, sched)
            // TODO: notify
        })
    }

    private class ScrapeStarter(val svc: ScrapeService, val key: ScrapeTargetKey, val sched: ScrapeSchedule) : IgniteRunnable {
        var closeToken: Closeable? = null // TODO: should be WeakRef?

        override fun run() {
            svc.doScrape(key)
        }
    }

    private fun schedule(now: LocalDateTime, key: ScrapeTargetKey, sched: ScrapeSchedule) {
        val scanLimit = now.plus(ScanRangeMilliseconds, ChronoUnit.MILLIS)
        if (sched.nextExec.isBefore(scanLimit)) {
            val starter = ScrapeStarter(this, key, sched)
            if (sched.nextExec.isBefore(now)) {
                ignite.scheduler().runLocal(starter)
            } else {
                val delay = now.until(sched.nextExec, ChronoUnit.MILLIS)
                val t = ignite.scheduler().runLocal(starter, delay, TimeUnit.MILLISECONDS)
                starter.closeToken = t
            }
        }
    }
}