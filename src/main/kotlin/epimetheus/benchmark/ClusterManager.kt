package epimetheus.benchmark

import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration

class ClusterManager(val serversCount: Int) {
    lateinit var servers: List<Ignite>
    lateinit var client: Ignite

    fun start(): Ignite {
        servers = (0 until serversCount).map {
            Ignition.start(IgniteConfiguration().apply {
                igniteInstanceName = "server-$it"
            })
        }
        client = Ignition.start(IgniteConfiguration().apply {
            igniteInstanceName = "client"
            isClientMode = true
        })
        return client
    }

    fun stop() {
        servers.forEach { it.close() }
        client.close()
    }
}