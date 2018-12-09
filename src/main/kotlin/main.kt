import epimetheus.EpimetheusServer
import epimetheus.prometheus.ConfigFile
import epimetheus.prometheus.Global
import epimetheus.prometheus.ScrapeConfig
import epimetheus.prometheus.StaticConfig
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import java.io.File
import java.time.Duration

fun main(args: Array<String>) {
    val cfg = Ignition.loadSpringBean<IgniteConfiguration>("conf/dev-config.xml", "grid.cfg")
    EpimetheusServer(cfg).use { s ->
        s.boot()
        s.applyLocalPrometheusConfig(File("conf/prometheus.yml"))
        while (true) {
            Thread.sleep(10000)
        }
    }
}
