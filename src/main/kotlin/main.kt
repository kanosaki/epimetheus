import epimetheus.Bootstrap
import epimetheus.prometheus.Config
import epimetheus.prometheus.Global
import epimetheus.prometheus.ScrapeConfig
import epimetheus.prometheus.StaticConfig
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import java.time.Duration

fun main(args: Array<String>) {
    startIgnite()
}

fun startIgnite() {
    Ignition.start("conf/dev-config.xml").use { ignite ->
        run(ignite)
    }
}

fun run(ignite: Ignite) {
    val localhost = ScrapeConfig("localhost",
            Duration.ofSeconds(10),
            null, null, null, null,
            listOf(StaticConfig(listOf("localhost:9100"), mapOf())))
    val scrape = ScrapeConfig(
            "remote", Duration.ofSeconds(10), "/federate", true, null,
            mapOf("match[]" to listOf("{__name__=~\"node_.*\"}")),
            listOf(StaticConfig(listOf("10.1.1.10:9090"), mapOf())))
    val b = Bootstrap(Config(listOf(scrape), Global(Duration.ofSeconds(15))), ignite)
    b.run()
}