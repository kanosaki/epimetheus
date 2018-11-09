package epimetheus.e2e

import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.prometheus.APIServerConfiguration
import epimetheus.prometheus.api.APIHandlerFactory
import epimetheus.prometheus.api.APIVerticle
import epimetheus.storage.MockGateway
import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.codec.BodyCodec
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import kotlin.test.assertEquals

@ExtendWith(VertxExtension::class)
class BasicE2ETests {
    companion object {
        val port = 23456
    }

    @BeforeEach
    fun setUp(vertx: Vertx, context: VertxTestContext) {
        val gateway = MockGateway()
        gateway.pushScraped("host1", 1000L, listOf(
                ScrapedSample.create("foo", 1.0, "a" to "1"),
                ScrapedSample.create("foo", 2.0, "b" to "1"),
                ScrapedSample.create("baz", 3.0, "a" to "1"),
                ScrapedSample.create("baz", 4.0, "b" to "1")
        ))
        gateway.pushScraped("host1", 2000L, listOf(
                ScrapedSample.create("foo", 5.0, "a" to "1"),
                ScrapedSample.create("foo", 6.0, "b" to "1"),
                ScrapedSample.create("baz", 7.0, "a" to "1"),
                ScrapedSample.create("baz", 8.0, "b" to "1")
        ))
        val handlerFactory = APIHandlerFactory(gateway)
        vertx.deployVerticle(
                APIVerticle(handlerFactory, APIServerConfiguration(port)),
                context.succeeding {
                    context.completeNow()
                })
    }


    @Test
    fun testRestBasic(vertx: Vertx, context: VertxTestContext) {
        val client = WebClient.create(vertx)
        client.get(port, "localhost", "/api/v1/query_range?query=foo&start=1&end=3&step=1")
                .`as`(BodyCodec.jsonObject())
                .send(context.succeeding { resp ->
                    context.verify {
                        assertEquals(200, resp.statusCode())
                        val o = resp.body()
                        assertEquals("success", o.getString("status"))
                        context.completeNow()
                    }
                })
    }

}