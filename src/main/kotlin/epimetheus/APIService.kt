package epimetheus

import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import epimetheus.api.APIVerticle
import epimetheus.api.EpimetheusAPIRoutes
import epimetheus.api.RouteConfigurator
import epimetheus.prometheus.api.PrometheusAPIRoutes
import epimetheus.prometheus.configfile.APIServerConfig
import epimetheus.storage.Gateway
import epimetheus.storage.IgniteGateway
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.Json
import org.apache.ignite.Ignite
import org.apache.ignite.resources.IgniteInstanceResource
import org.apache.ignite.services.Service
import org.apache.ignite.services.ServiceContext

class APIService : Service {
    @IgniteInstanceResource
    lateinit var ignite: Ignite

    lateinit var gateway: Gateway
    lateinit var vertx: Vertx
    lateinit var config: APIServerConfig
    lateinit var routes: List<RouteConfigurator>

    override fun init(ctx: ServiceContext?) {
        Json.mapper.registerKotlinModule()

        vertx = Vertx.vertx()
        val configCache = ignite.cache<String, APIServerConfig>(CacheName.CONFIG)
        config = configCache.get(ConfigKey.API_SERVER) ?: APIServerConfig(9090, 10)
        gateway = IgniteGateway(ignite)
        routes = listOf(
                PrometheusAPIRoutes(vertx, gateway),
                EpimetheusAPIRoutes(vertx, ignite)
        )
        println("Listening API at :${config.port}")
    }

    override fun cancel(ctx: ServiceContext?) {
        vertx.close()
    }

    override fun execute(ctx: ServiceContext?) {
        val self = this
        vertx.deployVerticle({
            APIVerticle(routes, config)
        }, DeploymentOptions().apply {
            isWorker = true
            instances = self.config.workers
        })
    }
}
