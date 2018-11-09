import epimetheus.Bootstrap
import epimetheus.prometheus.*
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
    val b = Bootstrap(
            Config(listOf(
                    ScrapeConfig("teor_in",
                            Duration.ofSeconds(10),
                            null, null, null, null,
                            listOf(StaticConfig(listOf("10.1.1.10:9100"), mapOf())))
            ), Global(Duration.ofSeconds(15))), ignite)
    b.run()
}