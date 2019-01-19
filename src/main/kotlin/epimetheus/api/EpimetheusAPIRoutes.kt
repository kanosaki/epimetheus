package epimetheus.api

import epimetheus.prometheus.scrape.ScrapeGateway
import io.vertx.core.Vertx
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.Json
import io.vertx.ext.web.Router
import org.apache.ignite.Ignite


class EpimetheusAPIRoutes(val vertx: Vertx, val ignite: Ignite) : RouteConfigurator {
    override fun configure(router: Router) {
        router.mountSubRouter("/epi/v1/cluster", clusterApis())
        router.mountSubRouter("/epi/v1/job", jobApis(ignite))
    }

    private fun clusterApis(): Router {
        return Router.router(vertx).apply {
            route("/")
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
            route("/nodes")
                    .handler { ctx ->
                        val r = ctx.response()
                        val nodes = ignite.cluster().nodes().map { NodeInfo(it, true) }
                        r.end(Json.encode(nodes))
                    }
            route("/cache/:name")
                    .handler { ctx ->
                        val r = ctx.response()
                        val c = ignite.cache<Any, Any>(ctx.pathParam("name"))
                        r.end(Json.encode(c.metrics()))
                    }
            route("/storage")
                    .handler { ctx ->
                        val r = ctx.response()
                        r.end(Json.encode(StorageInfo(ignite.dataStorageMetrics(), ignite.dataRegionMetrics())))
                    }
        }
    }

    private fun jobApis(ignite: Ignite): Router {
        val scrape = ScrapeGateway(ignite)
        return Router.router(vertx).apply {
            route("/status")
                    .handler { ctx ->
                        val r = ctx.response()
                        val statuses = scrape.targets.map { t ->
                            val status = scrape.statuses.get(t.key)
                            ScrapeStatus(t.key, t.value, status)
                        }
                        r.end(Json.encode(statuses))
                    }
            route(HttpMethod.GET, "/discovery")
                    .handler { ctx ->
                        val r = ctx.response()
                        val m = scrape.discoveries.map { entry ->
                            entry.value
                        }
                        r.end(Json.encode(m))
                    }
        }
    }
}