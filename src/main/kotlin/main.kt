import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody
import epimetheus.EpimetheusServer
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import java.io.File

fun main(rawArgs: Array<String>) = mainBody {
    val args = ArgParser(rawArgs).parseInto(::Args)
    val cfg = Ignition.loadSpringBean<IgniteConfiguration>(args.igniteXml, args.beanName)
    EpimetheusServer(cfg).use { s ->
        s.boot()
        if (args.prometheusConfig != null) {
            s.applyLocalPrometheusConfig(File(args.prometheusConfig))
        }
        while (true) {
            Thread.sleep(10000)
        }
    }
}

class Args(parser: ArgParser) {
    val prometheusConfig by parser
            .storing("--prometheus-config", help = "Path to prometheus configuration")
            .default<String?>(null)

    val igniteXml by parser
            .positional("Path for Ignite xml configuration file.")

    val beanName by parser
            .positional("IgniteConfiguration Bean name in configuration")
}
