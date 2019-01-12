package epimetheus.api

import epimetheus.prometheus.scrape.ScrapeSchedule
import epimetheus.prometheus.scrape.ScrapeTarget
import epimetheus.prometheus.scrape.ScrapeTargetKey
import org.apache.ignite.DataRegionMetrics
import org.apache.ignite.DataStorageMetrics
import org.apache.ignite.cluster.ClusterMetrics
import org.apache.ignite.cluster.ClusterNode
import java.io.Serializable

// Temporary models for rendering good json

class ClusterInfo(
        val nodes: List<NodeInfo>,
        val cacheNames: Collection<String>,
        val topVer: Long,
        val metrics: ClusterMetrics
        ) : Serializable

class NodeInfo(node: ClusterNode, withMetric: Boolean) : Serializable {
    val hostnames = node.hostNames()
    val addresses = node.addresses()
    val id = node.id()
    val metrics = if (withMetric) node.metrics() else null
    val version = "${node.version().major()}.${node.version().minor()}.${node.version().maintenance()}"
    val fullVersion = node.version().toString()
    val isClient = node.isClient
    val isDaemon = node.isDaemon
    val isLocal = node.isLocal
}

class StorageInfo(val storageMetrics: DataStorageMetrics?, val dataRegionMetrics: Collection<DataRegionMetrics>): Serializable

class ScrapeStatus(val key: ScrapeTargetKey, val target: ScrapeTarget, val schedule: ScrapeSchedule?): Serializable