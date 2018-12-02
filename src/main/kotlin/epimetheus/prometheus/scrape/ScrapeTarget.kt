package epimetheus.prometheus.scrape

import org.apache.ignite.cache.affinity.AffinityKeyMapped


data class ScrapeTargetName(
        val confName: String,
        @AffinityKeyMapped val target: String)

class ScrapeTarget(val jobName: String, val url: String, val intervalSeconds: Float, val honorLabels: Boolean, val params: Map<String, List<String>>) {

}
