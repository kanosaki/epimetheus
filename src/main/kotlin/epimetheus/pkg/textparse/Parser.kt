package epimetheus.pkg.textparse

import epimetheus.model.Metric
import epimetheus.model.MetricBuilder
import epimetheus.pkg.promql.PromQL
import epimetheus.pkg.promql.PromQLException
import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.CommonTokenStream


class InvalidSpecLine(msg: String) : RuntimeException(msg)


data class ScrapedSample(val met: Metric, val value: Double, val timestamp: Long? = null) {
    companion object {
        val m = Metric.of("__ignored__")
        val Ignored = ScrapedSample(m, 0.0)

        fun create(name: String, value: Double, vararg pairs: Pair<String, String>): ScrapedSample {
            val mb = MetricBuilder()
            mb.put(Metric.nameLabel, name)
            for (i in 0 until pairs.size) {
                mb.put(pairs[i].first, pairs[i].second)
            }
            return ScrapedSample(mb.build(), value)
        }
    }

    override fun toString(): String {
        val nonNameLabels = met.labels().filter { e -> e.first != Metric.nameLabel }
        return "${met.get(Metric.nameLabel)}$nonNameLabels $value ${timestamp ?: ""}"
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
                return (sign * iNum.text.toDouble())
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

    class MetricVisitor : PromExporterParserBaseVisitor<Metric>() {
        override fun visitMetric(ctx: PromExporterParser.MetricContext?): Metric {
            val mb = MetricBuilder()
            val name = ctx!!.metricName().text
            mb.put(Metric.nameLabel, name)
            if (ctx.labelBrace() != null) {
                ctx.labelBrace().label().forEach { c ->
                    mb.put(c.NAME().text, c.stringLiteral().STRINGCONTENT().text.removeSuffix("\""))
                }
            }
            return mb.build()
        }
    }

    class SampleVisitor : PromExporterParserBaseVisitor<ScrapedSample>() {
        override fun visitSample(ctx: PromExporterParser.SampleContext?): ScrapedSample {
            val met = metricVisitor.visit(ctx!!.metric())
            val v = ctx.value() ?: return ScrapedSample.Ignored
            val value = valueVisitor.visit(v)
            val tsStr = ctx.timestamp()?.integer()?.NUMBER()?.text
            return if (value == null) {
                ScrapedSample.Ignored
            } else {
                if (tsStr == null) {
                    ScrapedSample(met, value)
                } else {
                    ScrapedSample(met, value, tsStr.toLong())
                }
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

