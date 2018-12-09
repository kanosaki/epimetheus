package epimetheus.prometheus.scrape

import epimetheus.CacheName.Prometheus.SCRAPE_STATUSES
import epimetheus.CacheName.Prometheus.SCRAPE_TARGETS
import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.storage.IgniteGateway
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.CacheInterceptorAdapter
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.lang.IgniteRunnable
import org.apache.ignite.resources.IgniteInstanceResource
import org.apache.ignite.services.Service
import org.apache.ignite.services.ServiceContext
import java.io.Closeable
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import javax.cache.Cache

interface ScrapeStatus
data class ScrapeStatusSuccess(val latencyNs: Long) : ScrapeStatus
data class ScrapeStatusFailure(val cause: Throwable) : ScrapeStatus

data class ScrapeResult(val latencyNs: Long, val samples: List<ScrapedSample>)

data class ScrapeSchedule(
        val nextExec: LocalDateTime,
        val lastTimestamp: LocalDateTime?,
        val lastStatus: ScrapeStatus?
)


class ScrapeService() : Service {
    companion object {
        val ScanRangeMilliseconds = 10 * 1000L
    }

    @IgniteInstanceResource
    lateinit var ignite: Ignite
    lateinit var vertx: Vertx
    lateinit var client: WebClient
    lateinit var storage: IgniteGateway
    private var cancelled = false

    lateinit var statusConf: CacheConfiguration<ScrapeTargetKey, ScrapeSchedule>
    lateinit var statuses: IgniteCache<ScrapeTargetKey, ScrapeSchedule>
    lateinit var targetConf: CacheConfiguration<ScrapeTargetKey, ScrapeTarget>
    lateinit var targets: IgniteCache<ScrapeTargetKey, ScrapeTarget>

    class StatusCacheInterceptor() : CacheInterceptorAdapter<ScrapeTargetKey, ScrapeSchedule>() {
        // TODO: notify?
        //override fun onBeforePut(entry: Cache.Entry<ScrapeTargetKey, ScrapeSchedule>?, newVal: ScrapeSchedule?): ScrapeSchedule? {
        //    if (newVal == null) return newVal
        //    svc.schedule(LocalDateTime.now(), entry!!.key, newVal)
        //    return newVal
        //}
    }

    class TargetCacheInterceptor(val statuses: IgniteCache<ScrapeTargetKey, ScrapeSchedule>) : CacheInterceptorAdapter<ScrapeTargetKey, ScrapeTarget>() {
        override fun onAfterPut(entry: Cache.Entry<ScrapeTargetKey, ScrapeTarget>?) {
            // use async! to prevent striped executor blocked
            statuses.putAsync(entry!!.key, ScrapeSchedule(LocalDateTime.now(), null, null))
        }

        override fun onAfterRemove(entry: Cache.Entry<ScrapeTargetKey, ScrapeTarget>?) {
            statuses.removeAsync(entry!!.key) // cascade delete, use async!
        }
    }

    private fun writeSamples(target: ScrapeTargetKey, cfg: ScrapeTarget, results: List<ScrapedSample>) {
        storage.pushScraped(target.target, System.currentTimeMillis(), results.map {
            val mb = it.met.builder()
            if (!cfg.honorLabels) {
                mb.put("instance", target.target)
                mb.put("job", target.jobName)
            }
            ScrapedSample(mb.build(), it.value)
        })
    }

    override fun init(ctx: ServiceContext?) {
        vertx = Vertx.vertx()
        client = WebClient.create(vertx)
        storage = IgniteGateway(ignite)

        statusConf = CacheConfiguration<ScrapeTargetKey, ScrapeSchedule>().apply {
            name = SCRAPE_STATUSES
            backups = 1
            interceptor = StatusCacheInterceptor()
        }
        statuses = ignite.getOrCreateCache<ScrapeTargetKey, ScrapeSchedule>(statusConf)

        targetConf = CacheConfiguration<ScrapeTargetKey, ScrapeTarget>().apply {
            name = SCRAPE_TARGETS
            backups = 1
            interceptor = TargetCacheInterceptor(statuses)
        }
        targets = ignite.getOrCreateCache<ScrapeTargetKey, ScrapeTarget>(targetConf)
    }

    override fun cancel(ctx: ServiceContext?) {
        cancelled = true
    }

    override fun execute(ctx: ServiceContext?) {
        while (!cancelled) {
            val now = LocalDateTime.now()
            statuses.localEntries().map { kv ->
                schedule(now, kv.key, kv.value)
            }
            Thread.sleep(ScanRangeMilliseconds)
        }
    }

    fun doScrape(key: ScrapeTargetKey) {
        val cfg = targets.get(key)
        val scr = Scraper(client, cfg)
        vertx.executeBlocking<ScrapeResult>(scr, false, Handler { ar ->
            val finishedAt = LocalDateTime.now()
            val status = when (ar.succeeded()) {
                true -> {
                    val samples = ar.result().samples
                    println("SCRAPED ${cfg.url} ${samples.size} samples")
                    writeSamples(key, cfg, samples)
                    ScrapeStatusSuccess(ar.result().latencyNs)
                }
                false -> {
                    println("SCRAPE FAILED ${cfg.url} ${ar.cause()}")
                    ScrapeStatusFailure(ar.cause())
                }
            }
            val sched = ScrapeSchedule(
                    finishedAt.plusNanos((cfg.intervalSeconds * 1e9).toLong()),
                    finishedAt,
                    status
            )
            statuses.put(key, sched)
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