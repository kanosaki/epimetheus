package epimetheus.prometheus.scrape

import epimetheus.pkg.textparse.ScrapedSample
import org.apache.ignite.cache.affinity.AffinityKeyMapped
import org.apache.ignite.cache.query.annotations.QuerySqlField
import java.time.LocalDateTime


data class ScrapeTargetKey(
        @QuerySqlField val jobName: String,
        @QuerySqlField @AffinityKeyMapped val target: String)

class ScrapeTarget(
        val url: String,
        val intervalSeconds: Float,
        val honorLabels: Boolean,
        val params: Map<String, List<String>>)

interface ScrapeStatus
data class ScrapeStatusSuccess(val latencyNs: Long) : ScrapeStatus
data class ScrapeStatusFailure(val cause: Throwable) : ScrapeStatus

data class ScrapeResult(val latencyNs: Long, val samples: List<ScrapedSample>)

data class ScrapeSchedule(
        val nextExec: LocalDateTime,
        val lastTimestamp: LocalDateTime?,
        val lastStatus: ScrapeStatus?
)


