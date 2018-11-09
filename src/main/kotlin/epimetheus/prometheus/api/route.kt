package epimetheus.prometheus.api

import epimetheus.engine.Interpreter
import epimetheus.model.TimeFrames
import epimetheus.prometheus.api.Util.commonFailureHandler
import epimetheus.prometheus.api.Util.queryDuration
import epimetheus.prometheus.api.Util.queryString
import epimetheus.prometheus.api.Util.queryTimeParam
import epimetheus.prometheus.api.Util.toResult
import epimetheus.storage.Gateway
import io.vertx.core.json.Json
import io.vertx.ext.web.Router


class APIHandlerFactory(val gateway: Gateway) {

    // NOTE: https://prometheus.io/docs/prometheus/latest/querying/api/
    // GET /api/v1/query_range
    // GET /api/v1/query
    // GET /api/v1/series
    // GET /api/v1/targets
    // GET /api/v1/rules
    // GET /api/v1/alerts
    // GET /api/v1/label/<label_name>/values
    // GET /api/v1/targets/metadata
    // GET /api/v1/alertmanagers
    // GET /api/v1/status/config
    // GET /api/v1/status/flags

    // Admin APIs
    // POST /api/v1/admin/tsdb/snapshot?skip_head=<bool>
    // POST /api/v1/admin/tsdb/delete_series
    // POST /api/v1/admin/tsdb/clean_tombstones

    fun configure(router: Router) {
        router.route("/api/v1/query")
                .handler {c ->
                    // no nothing but just for Grafana datasource check.
                    val r = c.response()
                    r.end(Json.encode(Response("success", Result("string", "ok"), null, null)))
                }
        router.route("/api/v1/query_range")
                .handler { c ->
                    val query = queryString(c, "query")
                    val start = queryTimeParam(c, "start")
                    val end = queryTimeParam(c, "end")
                    val step = queryDuration(c, "step")
                    val interp = Interpreter(gateway)
                    val r = c.response()
                    val value = interp.eval(query, TimeFrames(start.toEpochMilli(), end.toEpochMilli(), step.toMillis()))
                    val result = toResult(value)
                    r.end(Json.encode(Response("success", result, null, null)))
                }
                .failureHandler(commonFailureHandler)

        router.route("/api/v1/series")
                .handler { c ->
                    val matches = c.queryParam("match[]")
                    val start = queryTimeParam(c, "start")
                    val end = queryTimeParam(c, "end")
                    TODO()
                }
        router.exceptionHandler { ev ->
            println("${ev.message}")
        }
    }
}

