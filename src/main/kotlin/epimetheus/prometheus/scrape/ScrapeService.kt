package epimetheus.prometheus.scrape

import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.storage.IgniteGateway
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
import org.apache.ignite.Ignite
import org.apache.ignite.resources.IgniteInstanceResource
import org.apache.ignite.services.Service
import org.apache.ignite.services.ServiceContext
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class ScrapeService : Service {
    companion object {
        val ScanRangeMilliseconds = 10 * 1000L
        val MaximumStreamThreads = 16
        val StreamJobQueueSize = 256
    }

    @IgniteInstanceResource
    lateinit var ignite: Ignite
    lateinit var vertx: Vertx
    lateinit var client: WebClient
    lateinit var storage: IgniteGateway
    lateinit var scrapeGate: ScrapeGateway

    lateinit var submitThread: ExecutorService


    override fun init(ctx: ServiceContext?) {
        submitThread = ThreadPoolExecutor(1, MaximumStreamThreads, 30, TimeUnit.SECONDS, LinkedBlockingQueue<Runnable>(StreamJobQueueSize))
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
            scrapeGate.nodeAssignedTargets().map { kv ->
                val status = scrapeGate.statuses.get(kv.key)
                val scanLimit = now.plus(ScanRangeMilliseconds, ChronoUnit.MILLIS)
                if (status.nextExec.isBefore(scanLimit)) {
                    // TODO: flow control
                    if (status.nextExec.isBefore(now)) {
                        vertx.runOnContext {
                            doScrape(kv.key, kv.value)
                        }
                    } else {
                        val delay = now.until(status.nextExec, ChronoUnit.MILLIS)
                        vertx.setTimer(delay) {
                            doScrape(kv.key, kv.value)
                        }
                    }
                }
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

    private fun doScrape(key: ScrapeTargetKey, cfg: ScrapeTarget) {
        val log = ignite.log()
        val scr = Scraper(client, cfg)
        vertx.executeBlocking<ScrapeResponse>(scr, false, Handler { ar ->
            val finishedAt = LocalDateTime.now()
            val status = when (ar.succeeded()) {
                true -> {
                    val samples = ar.result().samples
                    log.info("SCRAPED $key ${samples.size} samples")
                    // process background to avoid blocking vert.x event loop thread
                    submitThread.submit {
                        writeSamples(key, cfg, samples)
                        updateTargetStatus(key, true)
                    }
                    ScrapeResultSuccess(ar.result().latencyNs)
                }
                false -> {
                    log.error("SCRAPE FAILED $key ${ar.cause()}")
                    submitThread.submit {
                        updateTargetStatus(key, false)
                    }
                    ScrapeResultFailure(ar.cause())
                }
            }
            val sched = ScrapeStatus(
                    finishedAt.plusNanos((cfg.intervalSeconds * 1e9).toLong()),
                    finishedAt,
                    status
            )
            scrapeGate.updateStatus(key, sched)
        })
    }
}