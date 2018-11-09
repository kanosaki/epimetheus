package epimetheus.prometheus.api

import epimetheus.prometheus.APIServerConfiguration
import io.vertx.core.AbstractVerticle
import io.vertx.core.http.HttpServer
import io.vertx.ext.web.Router

class APIVerticle(val handlerFactory: APIHandlerFactory, val config: APIServerConfiguration) : AbstractVerticle() {
    lateinit var server: HttpServer
    override fun start() {
        val router = Router.router(vertx)
        router.route().handler { rc ->
            rc.addBodyEndHandler {
                // TODO: use better logger
                println("${rc.response().statusCode} ${rc.request().method()} ${rc.request().path()}")
            }
            rc.next()
        }
        handlerFactory.configure(router)
        server = vertx.createHttpServer()
                .requestHandler { router.accept(it) }
                .listen(config.port)
    }

    override fun stop() {
        server.close()
    }
}