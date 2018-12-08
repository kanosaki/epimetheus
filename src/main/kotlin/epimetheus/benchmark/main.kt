package epimetheus.benchmark

import epimetheus.pkg.promql.PromQL
import org.antlr.v4.runtime.CharStreams

fun main(args: Array<String>) {
    val s = """sum (rate(node_cpu{mode!='idle',mode!='user',mode!='system',mode!='iowait',mode!='irq',mode!='softirq',instance=~"undefined:undefined"}[5m])) * 100"""
    while (true) {
        val cs = CharStreams.fromString(s)
        val ast = PromQL.parse(cs, false)
    }
}