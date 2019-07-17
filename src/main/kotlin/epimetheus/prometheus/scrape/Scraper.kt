package epimetheus.prometheus.scrape

import epimetheus.pkg.textparse.ExporterParser
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Promise
import io.vertx.ext.web.client.WebClient
import org.antlr.v4.runtime.CharStreams

class Scraper(private val client: WebClient, private val target: ScrapeTarget) : Handler<Promise<ScrapeResponse>> {
    override fun handle(event: Promise<ScrapeResponse>?) {
        assert(event != null)
        val begin = System.nanoTime()
        val req = client.getAbs(target.url)
        target.params.forEach { k, v ->
            req.queryParams().add(k, v)
        }
        req.send { ar ->
            val elapsed = System.nanoTime() - begin
            if (ar.failed()) {
                event?.fail(ar.cause())
                return@send
            }
            val resp = ar.result()
            val s = resp.bodyAsString()
            val samples = ExporterParser.parse(CharStreams.fromString(s))
            event?.complete(ScrapeResponse(elapsed, samples))
        }
    }
}

