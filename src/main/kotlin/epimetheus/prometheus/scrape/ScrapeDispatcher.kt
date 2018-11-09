package epimetheus.prometheus.scrape

import epimetheus.CacheName.Prometheus.SCRAPE_TARGETS
import epimetheus.ScrapeResult
import epimetheus.ScrapeSchedule
import epimetheus.ScrapeStatusFailure
import epimetheus.ScrapeStatusSuccess
import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.storage.IgniteGateway
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.resources.IgniteInstanceResource
import org.apache.ignite.services.Service
import org.apache.ignite.services.ServiceContext
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import kotlin.math.max

class ScrapeDispatcher : Service {
    @IgniteInstanceResource
    lateinit var ignite: Ignite
    lateinit var vertx: Vertx
    lateinit var client: WebClient

    lateinit var targets: IgniteCache<ScrapeTargetName, ScrapeTarget>
    lateinit var statuses: ScrapeStatusCache
    lateinit var storage: IgniteGateway

    private val queue = PriorityBlockingQueue<ScrapeSchedule>(64, Comparator<ScrapeSchedule> { o1, o2 ->
        o1.nextExec.compareTo(o2.nextExec)
    })
    private var cancelled = false

    fun fillQueue() {
        queue.clear()
        queue.addAll(statuses.restoreStatuses())
    }

    override fun init(ctx: ServiceContext?) {
        statuses = ScrapeStatusCache(ignite)
        targets = ignite.cache<ScrapeTargetName, ScrapeTarget>(SCRAPE_TARGETS)
        storage = IgniteGateway(ignite)
        vertx = Vertx.vertx()
        client = WebClient.create(vertx)
        fillQueue()
    }

    override fun cancel(ctx: ServiceContext?) {
        cancelled = true
    }

    private fun writeSamples(target: ScrapeTargetName, results: List<ScrapedSample>) {
        storage.pushScraped(target.target, System.currentTimeMillis(), results)
    }

    override fun execute(ctx: ServiceContext?) {
        while (!cancelled) {
            val now = LocalDateTime.now()
            while (true) {
                val head = queue.poll()
                if (head == null) {
                    Thread.sleep(10000)
                    fillQueue()
                    break
                }
                val deltaMillis = ChronoUnit.MILLIS.between(now, head.nextExec)
                if (deltaMillis > 0) {
                    queue.add(head)
                    println("SLEEPING ${max(deltaMillis, 1000)}")
                    Thread.sleep(max(deltaMillis, 1000))
                    break
                } else {
                    val cfg = targets.get(head.key)
                    val scr = Scraper(client, cfg)
                    vertx.executeBlocking<ScrapeResult>(scr, false, Handler { ar ->
                        val finishedAt = LocalDateTime.now()
                        val status = when (ar.succeeded()) {
                            true -> {
                                val samples = ar.result().samples
                                println("SCRAPED ${cfg.url} ${samples.size} samples")
                                writeSamples(head.key, samples)
                                ScrapeStatusSuccess(ar.result().latencyNs)
                            }
                            false -> {
                                println("SCRAPE FAILED ${cfg.url} ${ar.cause()}")
                                ScrapeStatusFailure(ar.cause())
                            }
                        }
                        val sched = ScrapeSchedule(
                                head.key,
                                finishedAt.plusNanos((cfg.intervalSeconds * 1e9).toLong()),
                                finishedAt,
                                status
                        )
                        queue.add(sched)
                        statuses.update(sched)
                    })
                }
            }
        }
    }
}

