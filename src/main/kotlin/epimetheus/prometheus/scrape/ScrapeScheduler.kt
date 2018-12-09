package epimetheus.prometheus.scrape

import org.apache.ignite.cache.affinity.AffinityKeyMapped
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import java.sql.Timestamp


data class ScrapeTargetKey(
        @QuerySqlField val jobName: String,
        @QuerySqlField @AffinityKeyMapped val target: String)

class ScrapeTarget(
        val url: String,
        val intervalSeconds: Float,
        val honorLabels: Boolean,
        val params: Map<String, List<String>>) {
}

