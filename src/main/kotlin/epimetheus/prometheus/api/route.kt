package epimetheus.prometheus.api

import epimetheus.engine.Interpreter
import epimetheus.model.LabelMatchType
import epimetheus.model.LabelMatcher
import epimetheus.model.MetricMatcher
import epimetheus.model.TimeFrames
import epimetheus.pkg.promql.PromQL
import epimetheus.prometheus.api.Util.commonFailureHandler
import epimetheus.prometheus.api.Util.queryDuration
import epimetheus.prometheus.api.Util.queryString
import epimetheus.prometheus.api.Util.queryTimeParam
import epimetheus.prometheus.api.Util.toResult
import epimetheus.storage.Gateway
import epimetheus.storage.IgniteGateway
import epimetheus.storage.Meta
import io.vertx.core.Vertx
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.Json
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import org.antlr.v4.runtime.CharStreams
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopInputFile
import java.io.File


class APIHandlerFactory(val vertx: Vertx, val gateway: Gateway) {
    fun configure(router: Router) {
        router.mountSubRouter("/api/v1", prometheusV1RestApi())
        router.mountSubRouter("/api/v1", epimetheusDebuggingApi())
        router.exceptionHandler { ev ->
            println("${ev.message}")
        }
    }

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

    fun prometheusV1RestApi(): Router {
        return Router.router(vertx).apply {
            route("/query")
                    .handler { ctx ->
                        // no nothing but just for Grafana datasource check.
                        val r = ctx.response()
                        r.end(Json.encode(Response("success", Result("string", "ok"), null, null)))
                    }
            route("/label/:name/values")
                    .handler { ctx ->
                        val meta = gateway.metricRegistry as Meta
                        val name = ctx.pathParam("name") ?: throw InvalidRequestException("neve here")
                        val mets = meta.lookupMetrics(MetricMatcher(listOf(name to LabelMatcher(LabelMatchType.Match, ".*"))))
                        ctx.response().end(Json.encode(Response("success", mets.map { it.get(name) }, null, null)))
                    }
            route("/query_range")
                    .handler { ctx ->
                        val query = queryString(ctx, "query")
                        val start = queryTimeParam(ctx, "start")
                        val end = queryTimeParam(ctx, "end")
                        val step = queryDuration(ctx, "step")
                        val interp = Interpreter(gateway)
                        val r = ctx.response()
                        val value = interp.eval(query, TimeFrames(start.toEpochMilli(), end.toEpochMilli(), step.toMillis()))
                        val result = toResult(value)
                        r.end(Json.encode(Response("success", result, null, null)))
                    }
                    .failureHandler(commonFailureHandler)
            route("/series")
                    .handler { ctx ->
                        val matches = ctx.queryParam("match[]")
                        val start = queryTimeParam(ctx, "start")
                        val end = queryTimeParam(ctx, "end")
                        val matchers = matches.map { PromQL.parseInstantSelector(CharStreams.fromString(it)).matcher }
                        val r = ctx.response()
                        val startMs = start.toEpochMilli()
                        val endMs = end.toEpochMilli()
                        val mets = matchers.flatMap {
                            val rm = gateway.collectRange(it, TimeFrames.instant(endMs), endMs - startMs)
                            rm.metrics
                        }
                        r.end(Json.encode(Response("success", mets.map { it.toSortedMap() }, null, null)))
                    }
        }
    }

    fun epimetheusDebuggingApi(): Router {
        return Router.router(vertx).apply {
            route(HttpMethod.POST, "/scrape_data")
                    .handler(BodyHandler.create())
                    .handler { ctx ->
                        for (fu in ctx.fileUploads()) {
                            when (fu.name()) {
                                "data" -> {
                                    val f = File(fu.uploadedFileName())
                                    val inputFile = HadoopInputFile.fromPath(Path("file://" + f.canonicalPath), org.apache.hadoop.conf.Configuration())
                                    (gateway as IgniteGateway).importParquet(inputFile)
                                }
                                else -> throw InvalidRequestException("unknown name: ${fu.name()}")
                            }
                        }
                        ctx.response().end(Json.encode(Response("success", "done", null, null)))
                    }
        }
    }
}

