package epimetheus.prometheus

import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals

class PrometheusConfigFileTest {
    @Test
    fun parseSimple() {
        val doc = """
            global:
              scrape_interval: 15s
            scrape_configs:
              - job_name: default
                scrape_interval: 1m
                metrics_path: /metpath
                honor_labels: true
                scheme: https
                params:
                  x-foo: ["hoge"]
                static_configs:
                  - targets:
                      - "localhost:8080"
                    labels:
                      foo: bar
                      hoge: fuga
        """.trimIndent()
        val c = Parser.mapper.readValue<ConfigFile>(doc, ConfigFile::class.java)
        assertEquals(c, ConfigFile(
                listOf(ScrapeConfig(
                        "default",
                        Duration.ofMinutes(1),
                        "/metpath",
                        true,
                        "https",
                        mapOf("x-foo" to listOf("hoge")),
                        listOf(StaticConfig(
                                listOf("localhost:8080"),
                                mapOf("foo" to "bar", "hoge" to "fuga")
                        ))
                )),
                Global(Duration.ofSeconds(15))
        ))
    }
}