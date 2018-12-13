package epimetheus.prometheus

import epimetheus.CacheName
import epimetheus.ConfigKey
import epimetheus.prometheus.api.APIHandlerFactory
import epimetheus.prometheus.api.APIVerticle
import epimetheus.prometheus.configfile.APIServerConfig
import epimetheus.storage.Gateway
import epimetheus.storage.IgniteGateway
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import org.apache.ignite.Ignite
import org.apache.ignite.resources.IgniteInstanceResource
import org.apache.ignite.services.Service
import org.apache.ignite.services.ServiceContext


class IgniteAPI : Service {
    @IgniteInstanceResource
    lateinit var ignite: Ignite

    lateinit var gateway: Gateway
    lateinit var vertx: Vertx
    lateinit var handlerFactory: APIHandlerFactory
    lateinit var config: APIServerConfig


    override fun init(ctx: ServiceContext?) {
        vertx = Vertx.vertx()
        val configCache = ignite.cache<String, APIServerConfig>(CacheName.CONFIG)
        config = configCache.get(ConfigKey.API_SERVER) ?: APIServerConfig(9090, 10)
        gateway = IgniteGateway(ignite)
        handlerFactory = APIHandlerFactory(vertx, gateway)
        println("Listening API at :${config.port}")
    }

    override fun cancel(ctx: ServiceContext?) {
        vertx.close()
    }

    override fun execute(ctx: ServiceContext?) {
        val self = this
        vertx.deployVerticle({
            APIVerticle(handlerFactory, config)
        }, DeploymentOptions().apply {
            isWorker = true
            instances = self.config.workers
        })
    }
}