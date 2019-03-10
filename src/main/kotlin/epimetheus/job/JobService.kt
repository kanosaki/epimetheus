package epimetheus.job

import epimetheus.ClusterConfig
import org.apache.ignite.Ignite
import org.apache.ignite.resources.IgniteInstanceResource
import org.apache.ignite.services.Service
import org.apache.ignite.services.ServiceContext
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.*

class JobService(val pollIntervalMillisec: Long = 10 * 1000) : Service {
    companion object {
        val ScanRangeMilliseconds = 10 * 1000L
        val MaximumStreamThreads = 16
        val StreamJobQueueSize = 256
        val SleepIgnoreMilliseconds = 100L
    }

    @IgniteInstanceResource
    lateinit var ignite: Ignite

    lateinit var gateway: JobGateway
    lateinit var config: ClusterConfig
    lateinit var executor: ExecutorService

    private val activeJobs = ConcurrentHashMap<JobKey, Long>()

    private var isPaused = false
    private lateinit var zone: ZoneId

    override fun init(ctx: ServiceContext?) {
        executor = ThreadPoolExecutor(
                1,
                MaximumStreamThreads,
                30,
                TimeUnit.SECONDS,
                LinkedBlockingQueue<Runnable>(StreamJobQueueSize)
        )
        gateway = JobGateway(ignite)
        config = ClusterConfig(ignite)
        zone = config.timeZone
    }

    override fun cancel(ctx: ServiceContext?) {
    }

    fun setPause(pause: Boolean) {
        isPaused = pause
    }

    private fun timestamp(): Long {
        return ZonedDateTime.now(zone).toInstant().toEpochMilli()
    }

    override fun execute(ctx: ServiceContext?) {
        while (!ctx!!.isCancelled) {
            val jobs = gateway.listAssignedJobs(ScanRangeMilliseconds)
                    // filter out already queued or active jobs
                    .filter { entry -> !activeJobs.containsKey(entry.key) }
                    .toList()

            if (jobs.isEmpty() || isPaused) {
                Thread.sleep(pollIntervalMillisec)
                continue
            }

            jobs.forEach { activeJobs[it.key] = it.value.nextExec }

            for (entry in jobs) {
                val job = entry.value
                val key = entry.key
                val now = ZonedDateTime.now(zone).toInstant().toEpochMilli()
                val sleep = job.nextExec - now
                if (sleep > SleepIgnoreMilliseconds) {
                    Thread.sleep(sleep)
                }
                executor.submit {
                    if (ctx.isCancelled) { // recheck cancellation
                        // TODO: clearing activeJobs is needed?
                        return@submit
                    }
                    // `begin` differs from the time requested to begin, this is intended to relax cluster workload by delaying jobs.
                    val begin = timestamp()
                    try {
                        val res = job.fn.call(ignite) // TODO: handle result
                        when (res) {
                            JobExitStatus.Terminate -> {
                                gateway.update(key, Job(job.fn, job.intervalMsec, begin + job.intervalMsec, true, "", true))
                            }
                            else -> {
                                gateway.update(key, Job(job.fn, job.intervalMsec, begin + job.intervalMsec, true, ""))
                            }
                        }
                    } catch (t: Throwable) {
                        // TODO: implement exponential backoff retrying for transient failure
                        ignite.log().error("JobFail:${entry.key}", t)
                        gateway.update(key, Job(job.fn, job.intervalMsec, begin + job.intervalMsec, false, t.message))
                    } finally {
                        // job status (updated by `gateway.update`) and `activeJobs` are not transactional (but it should be consistent, might be bug of Ignite)
                        Thread.sleep(10)
                        activeJobs.remove(key)
                    }
                }
            }
        }
    }
}
