package epimetheus.api

import epimetheus.prometheus.configfile.APIServerConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.http.HttpServer
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.CorsHandler

class APIVerticle(val routes: List<RouteConfigurator>, val config: APIServerConfig) : AbstractVerticle() {
    lateinit var server: HttpServer
    override fun start() {
        val router = Router.router(vertx)
        router.route().handler { rc ->
            val begin = System.currentTimeMillis()
            rc.addBodyEndHandler {
                // TODO: use better logger
                val elapsed = System.currentTimeMillis() - begin
                println("${rc.response().statusCode} ${rc.request().method()} ${rc.request().path()} ${elapsed}ms")
            }
            rc.next()
        }

        router.route().handler(CorsHandler.create(config.corsOrigin))
        for (route in routes) {
            route.configure(router)
        }
        router.exceptionHandler { ev ->
            ev.printStackTrace()
        }
        server = vertx.createHttpServer()
                .requestHandler { router.accept(it) }
                .listen(config.port)
    }

    override fun stop() {
        server.close()
    }
}