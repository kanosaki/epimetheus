package epimetheus.api

import io.vertx.core.Vertx
import io.vertx.core.json.Json
import io.vertx.ext.web.Router
import org.apache.ignite.Ignite


class EpimetheusAPIRoutes(val vertx: Vertx, val ignite: Ignite) : RouteConfigurator {
    override fun configure(router: Router) {
        router.mountSubRouter("/epi/v1", epimetheusV1RestAPI())
    }

    fun epimetheusV1RestAPI(): Router {
        return Router.router(vertx).apply {
            route("/cluster")
                    .handler { ctx ->
                        val r = ctx.response()
                        val nodes = ignite.cluster().nodes().map { NodeInfo(it, false) }
                        val info = ClusterInfo(
                                nodes,
                                ignite.cacheNames(),
                                ignite.cluster().topologyVersion(),
                                ignite.cluster().metrics()!!
                        )
                        r.end(Json.encode(info))
                    }
            route("/cluster/nodes")
                    .handler { ctx ->
                        val r = ctx.response()
                        val nodes = ignite.cluster().nodes().map { NodeInfo(it, true) }
                        r.end(Json.encode(nodes))
                    }
            route("/cluster/cache/:name")
                    .handler { ctx ->
                        val r = ctx.response()
                        val c = ignite.cache<Any, Any>(ctx.pathParam("name"))
                        r.end(Json.encode(c.metrics()))
                    }
            route("/cluster/storage")
                    .handler { ctx ->
                        val r = ctx.response()
                        r.end(Json.encode(StorageInfo(ignite.dataStorageMetrics(), ignite.dataRegionMetrics())))
                    }
        }
    }
}