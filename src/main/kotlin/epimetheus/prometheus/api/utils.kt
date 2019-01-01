package epimetheus.prometheus.api

import epimetheus.engine.plan.RPointMatrix
import epimetheus.engine.plan.RScalar
import epimetheus.engine.plan.RString
import epimetheus.model.GridMat
import epimetheus.model.Scalar
import epimetheus.model.StringValue
import epimetheus.model.Value
import epimetheus.pkg.promql.PromQL
import epimetheus.pkg.promql.PromQLException
import io.vertx.core.Handler
import io.vertx.core.json.Json
import io.vertx.ext.web.RoutingContext
import org.antlr.v4.runtime.CharStreams
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException


data class Response(
        /**
         * "success" or "error"
         */
        val status: String,
        val data: Any?,
        val errorType: String?,
        val error: String?
)

data class Result(val resultType: String, val result: Any)

class InvalidRequestException(msg: String) : RuntimeException(msg)

data class SeriesResult(val metric: Map<String, String>, val values: List<List<Any>>)

object Util {
    val commonFailureHandler: Handler<RoutingContext> = Handler { c ->
        val resp = c.response()
        val f = c.failure()
        f.printStackTrace()
        println(f.message)
        resp.statusCode = when (f) {
            is InvalidRequestException -> 400
            else -> 500
        }
        resp.end(Json.encode(Response("error", null, f.javaClass.simpleName, f.message)))
    }


    fun toResult(v: Value): Result {
        return when (v) {
            is GridMat -> {
                val resultType = if (v.timestamps.size == 1) {
                    "vector"
                } else {
                    "matrix"
                }
                val series = mutableListOf<SeriesResult>()
                v.rows().forEach { mr ->
                    val values = mutableListOf<List<Any>>()
                    mr.timestamps.forEachIndexed { i, t ->
                        values += listOf(t.toDouble() / 1e3, mr.values[i].toString())
                    }
                    series += SeriesResult(mr.metric.toSortedMap(), values)
                }
                Result(resultType, series)
            }
            is Scalar -> {
                Result("scalar", v.value)
            }
            is StringValue -> {
                Result("string", v.value)
            }
            is RPointMatrix -> {
                val resultType = if (v.series[0].timestamps.size == 1) {
                    "vector"
                } else {
                    "matrix"
                }
                val series = mutableListOf<SeriesResult>()
                v.series.forEachIndexed { i, s ->
                    val values = mutableListOf<List<Any>>()
                    for (j in 0 until s.timestamps.size) {
                        values += listOf(s.timestamps[j]/1e3, s.values[j].toString())
                    }
                    series += SeriesResult(v.metrics[i].toSortedMap(), values)
                }
                Result(resultType, series)
            }
            is RScalar -> {
                Result("scalar", v.value)
            }
            is RString -> {
                Result("string", v.value)
            }
            else -> {
                throw RuntimeException("never here")
            }
        }
    }

    /**
     * Extract an Instant from query parameter with specified name. convert it as unix epoch seconds or RFC3339 representation
     */
    fun queryTimeParam(rc: RoutingContext, name: String): Instant {
        val exprs = rc.queryParam(name)
        if (exprs.size == 0) {
            throw InvalidRequestException("missing query parameter: $name")
        }
        val expr = exprs[0]
        val asUnixTime = expr.toDoubleOrNull()
        if (asUnixTime != null) {
            return Instant.ofEpochSecond(asUnixTime.toLong(), ((asUnixTime % 1) * 1e9).toLong())
        }
        try {
            return Instant.parse(expr)
        } catch (_: DateTimeParseException) {
        }
        throw InvalidRequestException("query parameter unsupported format: $name")
    }

    fun queryDuration(rc: RoutingContext, name: String): Duration {
        val exprs = rc.queryParam(name)
        if (exprs.size == 0) {
            throw InvalidRequestException("missing query parameter: $name")
        }
        val expr = exprs[0]
        try {
            return PromQL.parseDuration(CharStreams.fromString(expr))
        } catch (_: PromQLException) {
        }
        try {
            val seconds = expr.toDouble()
            return Duration.ofSeconds(seconds.toLong(), ((seconds % 1) * 1e9).toLong())
        } catch (_: NumberFormatException) {
        }
        throw InvalidRequestException("query parameter unsupported format: $name")
    }

    fun queryString(rc: RoutingContext, name: String): String {
        val exprs = rc.queryParam(name)
        if (exprs.size == 0) {
            throw InvalidRequestException("missing query parameter: $name")
        }
        return exprs[0]
    }
}

