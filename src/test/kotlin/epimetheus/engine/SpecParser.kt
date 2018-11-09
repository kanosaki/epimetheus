package epimetheus.engine

import epimetheus.model.Mat
import epimetheus.model.Metric
import epimetheus.pkg.promql.PromQL
import epimetheus.pkg.promql.PromQLException
import epimetheus.pkg.textparse.*
import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.CommonTokenStream
import java.util.*

interface SpecValue {
    val repeat: Int
    fun expand(currentValue: Double?): List<Double?>
}

data class VNumber(val initial: Double, val delta: Double, override val repeat: Int) : SpecValue {
    override fun expand(currentValue: Double?): List<Double?> {
        return List(repeat) { initial }
    }
}

data class VStale(override val repeat: Int) : SpecValue {
    override fun expand(currentValue: Double?): List<Double?> {
        return List(repeat) { Mat.StaleValue }
    }
}

data class VBlank(override val repeat: Int) : SpecValue {
    override fun expand(currentValue: Double?): List<Double?> {
        return List(repeat) { null }
    }
}


interface Spec {
    val series: List<SpecValue>
    fun expand(initValue: Double?): List<Double?> {
        var currentValue = initValue
        val ret = mutableListOf<Double?>()
        series.forEach {
            val values = it.expand(currentValue)
            ret.addAll(values)
            if (ret.isNotEmpty()) {
                currentValue = ret.last()
            }
        }
        return ret
    }
}

data class SpecLiteralDesc(override val series: List<SpecValue>) : Spec

data class SpecSeriesDesc(val name: String, val labels: Array<Pair<String, String>>, override val series: List<SpecValue>) : Spec {
    val m = if (name == "") labels.toMap().toSortedMap() else (labels + (Metric.nameLabel to name)).toMap().toSortedMap()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SpecSeriesDesc

        if (name != other.name) return false
        if (!Arrays.equals(labels, other.labels)) return false
        if (series != other.series) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + Arrays.hashCode(labels)
        result = 31 * result + series.hashCode()
        return result
    }
}

object SpecSeriesParser {
    private val specDescVisitor = SpecDescVisitor()
    private val specVisitor = SpecVisitor()

    class SpecDescVisitor : PromExporterParserBaseVisitor<SpecValue>() {

        override fun visitSpecBlanks(ctx: PromExporterParser.SpecBlanksContext?): SpecValue {
            var times = 1
            if (ctx!!.NAME() != null) {
                val timesStr = ctx.NAME().text
                if (timesStr.startsWith("x")) {
                    try {
                        times = timesStr.drop(1).toInt()
                    } catch (_: NumberFormatException) {
                        throw PromQLException("Invalid sample repeat: $timesStr")
                    }
                }
            }
            return VBlank(times)
        }

        override fun visitSpecValues(ctx: PromExporterParser.SpecValuesContext?): SpecValue {
            var times = 1
            if (ctx!!.integer() != null) {
                times = ctx.integer().text.toInt()
            }
            // 1.0 x 1 can handle normally (as float(1.0) times(x) int(1)
            // but 1.0 x1 and 1.0x1 will lexed as float(1.0) ident(x1), so handle it here
            if (ctx.NAME() != null) {
                val timesStr = ctx.NAME().text
                if (timesStr.startsWith("x")) {
                    try {
                        times = timesStr.drop(1).toInt()
                    } catch (_: NumberFormatException) {
                        throw PromQLException("Invalid sample repeat: $timesStr")
                    }
                } else {
                    throw PromQLException("Invalid sample repeat: $timesStr")
                }
            }
            if (ctx.STALE() != null) {
                return VStale(times)
            }
            var delta = 0.0
            val specDelta = ctx.specDelta()
            if (specDelta != null) {
                delta = ExporterParser.valueVisitor.visit(specDelta.value())!!
                delta *= when (ctx.specDelta().sign().text) {
                    "+" -> 1.0
                    "-" -> -1.0
                    else -> throw RuntimeException("never here")
                }
            }
            val v = ctx.value()
            val value = ExporterParser.valueVisitor.visit(v)
            return VNumber(value!!, delta, times)
        }
    }

    class SpecVisitor : PromExporterParserBaseVisitor<Spec>() {
        override fun visitSpecSeriesDesc(ctx: PromExporterParser.SpecSeriesDescContext?): Spec {
            val labels = mutableMapOf<String, String>()

            val name = ctx!!.metricName()?.text ?: ""
            val lBrace = ctx.labelBrace()
            val desc = ctx.specDesc() ?: throw InvalidSpecLine("not a specDesc")
            val series = desc.children.map { specDescVisitor.visit(it) }
            return if (name == "" && lBrace == null) {
                SpecLiteralDesc(series)
            } else {
                lBrace?.label()?.forEach { c ->
                    labels[c.NAME().text] = c.stringLiteral().STRINGCONTENT().text.removeSuffix("\"")
                }
                SpecSeriesDesc(name, labels.toList().toTypedArray(), series)
            }
        }
    }

    fun parseLine(cs: CharStream): Spec {
        val errs = PromQL.ANTLRErrorDetector()
        val lexer = PromExporterLexer(cs)
        lexer.removeErrorListeners()
        lexer.addErrorListener(errs)

        val tokenStream = CommonTokenStream(lexer)
        val parser = PromExporterParser(tokenStream)
        parser.removeErrorListeners()
        parser.addErrorListener(errs)
        val ret = specVisitor.visit(parser.specSeriesDesc())
        if (errs.hasError()) {
            throw InvalidSpecLine(errs.errorMessage()!!)
        } else {
            return ret
        }
    }
}
