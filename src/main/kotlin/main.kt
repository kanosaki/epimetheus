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
    val attrs = mutableMapOf<String, String>()
    if (args.attrPrometheusApi) {
        attrs["api"] = "1"
    }
    cfg.userAttributes = attrs
    cfg.isClientMode = args.clientMode
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

    val clientMode by parser
            .flagging("--client", help = "Path to prometheus configuration")

    val attrPrometheusApi by parser
            .flagging("--prometheus-api", help = "Path to prometheus configuration")

    val igniteXml by parser
            .positional("Path for Ignite xml configuration file.")

    val beanName by parser
            .positional("IgniteConfiguration Bean name in configuration")
}
