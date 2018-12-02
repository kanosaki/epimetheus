package epimetheus.pkg.textparse

import epimetheus.model.Metric
import epimetheus.pkg.promql.PromQL
import epimetheus.pkg.promql.PromQLException
import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.CommonTokenStream
import java.util.*


class InvalidSpecLine(msg: String) : RuntimeException(msg)


data class ScrapedSample(val m: SortedMap<String, String>, val value: Double) {
    companion object {
        val m = sortedMapOf(Metric.nameLabel to "__ignored__")
        val Ignored = ScrapedSample(m, 0.0)

        fun create(name: String, value: Double, vararg pairs: Pair<String, String>): ScrapedSample {
            return ScrapedSample(sortedMapOf(Metric.nameLabel to name, *pairs), value)
        }
    }

    override fun toString(): String {
        val nonNameLabels = m.filter { e -> e.key != Metric.nameLabel }
        return "${m[Metric.nameLabel]}$nonNameLabels $value"
    }
}

object ExporterParser {
    val valueVisitor = ValueVisitor()
    val sampleVisitor = SampleVisitor()
    val metricVisitor = MetricVisitor()

    class ExporterVisitor : PromExporterParserBaseVisitor<List<ScrapedSample>>() {
        override fun visitExporter(ctx: PromExporterParser.ExporterContext?): List<ScrapedSample> {
            return ctx!!.sample().map { sampleVisitor.visit(it) }
        }
    }

    class ValueVisitor : PromExporterParserBaseVisitor<Double?>() {
        override fun visitValue(ctx: PromExporterParser.ValueContext?): Double? {
            val v = ctx ?: return null
            val sign = when (v.sign()?.text) {
                "-" -> -1
                else -> 1
            }
            val iNum = v.integer()
            if (iNum != null) {
                return (sign * iNum.text.toInt()).toDouble()
            }
            val fNum = v.floatNum() ?: throw PromQLException("invalid state")
            return if (fNum.NaN() != null) {
                Double.NaN
            } else if (fNum.INF() != null) {
                if (sign == 1) {
                    Double.POSITIVE_INFINITY
                } else {
                    Double.NEGATIVE_INFINITY
                }
            } else {
                val value = fNum.text.toDoubleOrNull()
                if (value == null) {
                    return null
                } else {
                    sign * value
                }
            }
        }
    }

    class MetricVisitor: PromExporterParserBaseVisitor<Metric>() {
        override fun visitMetric(ctx: PromExporterParser.MetricContext?): Metric {
            val name = ctx!!.metricName().text
            val labels = mutableMapOf<String, String>()
            if (ctx.labelBrace() != null) {
                ctx.labelBrace().label().forEach { c ->
                    labels[c.NAME().text] = c.stringLiteral().STRINGCONTENT().text.removeSuffix("\"")
                }
            }
            labels[Metric.nameLabel] = name
            return Metric(labels.toSortedMap())
        }
    }

    class SampleVisitor : PromExporterParserBaseVisitor<ScrapedSample>() {
        override fun visitSample(ctx: PromExporterParser.SampleContext?): ScrapedSample {
            val met = metricVisitor.visit(ctx!!.metric())
            val v = ctx.value() ?: return ScrapedSample.Ignored
            val value = valueVisitor.visit(v)
            return if (value == null) {
                ScrapedSample.Ignored
            } else {
                ScrapedSample(met.m, value)
            }
        }
    }

    fun parseMetric(cs: CharStream, reportANTLRIssue: Boolean = PromQL.DEFAULT_ANTLR_REPORT): Metric {
        val errs = PromQL.ANTLRErrorDetector(reportANTLRIssue)
        val lexer = PromExporterLexer(cs)
        lexer.removeErrorListeners()
        lexer.addErrorListener(errs)

        val tokenStream = CommonTokenStream(lexer)
        val parser = PromExporterParser(tokenStream)
        parser.removeErrorListeners()
        parser.addErrorListener(errs)

        val res = metricVisitor.visit(parser.metric())
        if (errs.hasError()) {
            // TODO: use specialized exception
            throw RuntimeException(errs.errorMessage()!!)
        } else {
            return res
        }
    }


    fun parse(cs: CharStream, reportANTLRIssue: Boolean = PromQL.DEFAULT_ANTLR_REPORT): List<ScrapedSample> {
        val errs = PromQL.ANTLRErrorDetector(reportANTLRIssue)
        val lexer = PromExporterLexer(cs)
        lexer.removeErrorListeners()
        lexer.addErrorListener(errs)

        val tokenStream = CommonTokenStream(lexer)
        val parser = PromExporterParser(tokenStream)
        parser.removeErrorListeners()
        parser.addErrorListener(errs)

        val visitor = ExporterVisitor()
        val res = visitor.visit(parser.exporter())
        if (errs.hasError()) {
            // TODO: use specialized exception
            throw RuntimeException(errs.errorMessage()!!)
        } else {
            return res
        }
    }
}

