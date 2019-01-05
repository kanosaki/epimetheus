package epimetheus.api

import io.vertx.ext.web.Router

interface RouteConfigurator {
    fun configure(router: Router)
}