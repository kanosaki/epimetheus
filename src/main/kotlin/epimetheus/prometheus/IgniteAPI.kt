package epimetheus.prometheus

import epimetheus.prometheus.api.APIHandlerFactory
import epimetheus.prometheus.api.APIVerticle
import epimetheus.storage.Gateway
import epimetheus.storage.IgniteGateway
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
    lateinit var apiVerticle: APIVerticle

    override fun init(ctx: ServiceContext?) {
        vertx = Vertx.vertx()
        val configCache = ignite.getOrCreateCache<String, APIServerConfiguration>("api-config")
        val config = configCache.get("default") ?: APIServerConfiguration(9090)
        gateway = IgniteGateway(ignite)
        val handlerFactory = APIHandlerFactory(gateway)
        apiVerticle = APIVerticle(handlerFactory, config)
    }

    override fun cancel(ctx: ServiceContext?) {
        vertx.close()
    }

    override fun execute(ctx: ServiceContext?) {
        vertx.deployVerticle(apiVerticle)
    }
}