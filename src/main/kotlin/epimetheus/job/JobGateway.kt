package epimetheus.job

import epimetheus.ClusterConfig
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.CachePeekMode
import org.apache.ignite.configuration.CacheConfiguration
import java.time.Duration
import java.time.ZonedDateTime
import javax.cache.Cache


class JobGateway(val ignite: Ignite) {
    private val jobCacheConf: CacheConfiguration<JobKey, Job> = CacheConfiguration("job")
    val jobCache: IgniteCache<JobKey, Job>
    val config = ClusterConfig(ignite)
    val zone = config.timeZone

    val LOOKAHEAD_DURATION_MILLISECONDS = 10L * 1000

    init {
        jobCache = ignite.getOrCreateCache(jobCacheConf)
    }

    fun scheduleInterval(key: JobKey, fn: JobRunnable, interval: Duration) {
        val intervalMsec = interval.toMillis()
        val now = ZonedDateTime.now(zone).toInstant().toEpochMilli()
        jobCache.put(key, Job(fn, intervalMsec, now))
    }

    fun remove(key: JobKey) {
        jobCache.remove(key)
    }

    // for testing
    fun clear() {
        jobCache.clear()
    }

    fun listAssignedJobs(within: Long = LOOKAHEAD_DURATION_MILLISECONDS): Iterable<Cache.Entry<JobKey, Job>> {
        val until = ZonedDateTime.now(config.timeZone).toInstant().toEpochMilli() + within
        // returning sorted entries might be good for respect priority
        return jobCache.localEntries(CachePeekMode.PRIMARY).filter { it.value.nextExec < until && !it.value.terminated }.sortedBy { it.value.nextExec }
    }

    fun update(key: JobKey, job: Job) {
        val prev = jobCache.getAndPut(key, job)
        if (prev == null) {
            ignite.log().warning("Updating empty job: clearing $key")
            jobCache.remove(key)
        }
    }
}
