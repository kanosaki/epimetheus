package epimetheus.pkg.promql

import epimetheus.pkg.textparse.PromQLLexer
import epimetheus.pkg.textparse.PromQLParser
import epimetheus.pkg.textparse.PromQLParserBaseVisitor
import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.atn.ATNConfigSet
import org.antlr.v4.runtime.dfa.DFA
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*

object PromQL {
    private val literalVisitor = LiteralVisitor()
    private val durationVisitor = DurationVisitor()
    private val aggregatorGroupVisitor = AggregatorGroupVisitor()
    private val labelMatchOptsVisitor = LabelMatchOptsVisotr()
    private val labelGroupOptsVisitor = LabelGroupOptsVisotr()
    private val numberLiteralVisitor = NumberLiteralVisitor()

    class NumberLiteralVisitor : PromQLParserBaseVisitor<Double>() {
        override fun visitNumberLiteral(ctx: PromQLParser.NumberLiteralContext?): Double? {
            val v = ctx ?: return null
            val sign = when (v.sign()?.text) {
                "-" -> -1.0
                else -> 1.0
            }
            return sign * when {
                ctx.floatLiteral() != null -> {
                    this.visitFloatLiteral(ctx.floatLiteral()) ?: return null
                }
                ctx.integerLiteral() != null -> {
                    this.visitIntegerLiteral(ctx.integerLiteral()) ?: return null
                }
                else -> throw RuntimeException("never here")
            }
        }

        override fun visitIntegerLiteral(ctx: PromQLParser.IntegerLiteralContext?): Double? {
            return ctx!!.NUMBER()?.text?.toLong()?.toDouble()
        }

        override fun visitFloatLiteral(ctx: PromQLParser.FloatLiteralContext?): Double? {
            val i = ctx!!.Inf()
            val n = ctx.NaN()
            return when {
                ctx!!.NaN() != null -> Double.NaN
                ctx.Inf() != null -> Double.POSITIVE_INFINITY
                else -> return ctx.text.toDoubleOrNull()
            }
        }
    }

    class LiteralVisitor : PromQLParserBaseVisitor<Literal>() {
        override fun visitLiterals(ctx: PromQLParser.LiteralsContext?): Literal {
            if (ctx!!.numberLiteral() != null) {
                return NumberLiteral(numberLiteralVisitor.visit(ctx.numberLiteral())!!)
            }
            if (ctx.stringLiteral() != null) {
                return StringLiteral(ctx.text.trim('"'))
            }
            throw RuntimeException("Never here")
        }
    }

    class AggregatorGroupVisitor : PromQLParserBaseVisitor<AggregatorGroup>() {
        override fun visitAggregateGroup(ctx: PromQLParser.AggregateGroupContext?): AggregatorGroup {
            val by = ctx!!.By()
            return if (by != null) {
                val labels = ctx.labelListParen()?.labelList()?.label()?.map { it.text!! } ?: listOf()
                AggregatorGroup(AggregatorGroupType.By, labels)
            } else {
                val labels = ctx.labelListParen()?.labelList()?.label()?.map { it.text!! } ?: listOf()
                AggregatorGroup(AggregatorGroupType.Without, labels)
            }
        }
    }

    class DurationVisitor : PromQLParserBaseVisitor<Duration>() {
        private val suffixes = mapOf(
                "ns" to ChronoUnit.NANOS,
                "us" to ChronoUnit.MICROS, "µs" to ChronoUnit.MICROS, "μs" to ChronoUnit.MICROS,
                "ms" to ChronoUnit.MILLIS,
                "s" to ChronoUnit.SECONDS,
                "m" to ChronoUnit.MINUTES,
                "h" to ChronoUnit.HOURS,
                "d" to ChronoUnit.DAYS,
                "w" to ChronoUnit.WEEKS,
                "y" to ChronoUnit.YEARS)

        override fun visitDuration(ctx: PromQLParser.DurationContext?): Duration {
            val tx = ctx!!.text
            for ((suffix, unit) in suffixes) {
                if (tx.endsWith(suffix)) {
                    return Duration.of(tx.substring(0, tx.length - suffix.length).toLong(), unit)
                }
            }
            throw PromQLException("Invalid duration format: $tx")
        }
    }

    class LabelMatchOptsVisotr : PromQLParserBaseVisitor<LabelMatchOption?>() {
        override fun visitLabelMatchOp(ctx: PromQLParser.LabelMatchOpContext?): LabelMatchOption? {
            if (ctx == null) {
                return null
            }
            val labels = ctx.labelListParen().labelList()?.label()?.map { it.text!! } ?: listOf()
            return when (ctx.getChild(0).text.toLowerCase()) {
                "on" -> LabelMatchOption(LabelMatchOptionType.On, labels)
                "ignoring" -> LabelMatchOption(LabelMatchOptionType.Ignoring, labels)
                else -> throw AssertionError("never here")
            }
        }
    }

    class LabelGroupOptsVisotr : PromQLParserBaseVisitor<LabelGroupOption?>() {
        override fun visitLabelGroupOp(ctx: PromQLParser.LabelGroupOpContext?): LabelGroupOption? {
            if (ctx == null) {
                return null
            }
            val labels = ctx.labelListParen()?.labelList()?.label()?.map { it.text!! } ?: listOf()
            return when (ctx.getChild(0).text.toLowerCase()) {
                "group_left" -> LabelGroupOption(LabelGroupOptionType.Left, labels)
                "group_right" -> LabelGroupOption(LabelGroupOptionType.Right, labels)
                else -> throw AssertionError("never here")
            }
        }
    }

    class ExpressionVisitor(val binding: Binding) : PromQLParserBaseVisitor<Expression>() {
        override fun visitExpr(ctx: PromQLParser.ExprContext?): Expression {
            val atom = ctx!!.atom()
            val s = ctx.text
            if (atom != null) {
                return this.visit(atom)
            }
            if (ctx.expr(1) == null) { // parenthesis
                return this.visit(ctx.expr(0))
            } else {
                val lhs = ctx.expr(0)
                val rhs = ctx.expr(1)
                val opName = ctx.getChild(1).text.toLowerCase()
                val mods = ctx.binOpModifiers()
                val boolMod = mods?.boolOp()
                val match = mods?.labelMatchOp()
                val group = mods?.labelGroupOp()
                val op = binding.binaryOps[opName] ?: throw PromQLException("Binary operator $opName is invalid")
                val lmo = if (match == null) null else labelMatchOptsVisitor.visit(match)
                val lgo = if (group == null) null else labelGroupOptsVisitor.visit(group)
                val matchOn = lmo?.mode == LabelMatchOptionType.On
                val card = when (lgo?.mode) {
                    LabelGroupOptionType.Left -> VectorMatchingCardinality.ManyToOne
                    LabelGroupOptionType.Right -> VectorMatchingCardinality.OneToMany
                    else -> {
                        if (op.isSetOperator) {
                            VectorMatchingCardinality.ManyToMany
                        } else {
                            VectorMatchingCardinality.OneToOne
                        }
                    }
                }
                val ret = BinaryCall(op,
                        this.visit(lhs),
                        this.visit(rhs),
                        VectorMatching(
                                card,
                                lmo?.labels ?: listOf(),
                                matchOn,
                                lgo?.labels ?: listOf()))
                return if (boolMod != null) {
                    BoolConvert(ret)
                } else {
                    ret
                }
            }
        }

        override fun visitSelector(ctx: PromQLParser.SelectorContext?): Expression {
            val matches = ctx!!.labelBlock()?.labelMatchList()?.labelMatch()
                    ?.map {
                        val rhs = literalVisitor.visit(it.literals())
                        LabelMatch(
                                it.label().text,
                                it.labelOperators().text,
                                rhs
                        )
                    }
            val offset = if (ctx.offset() == null) Duration.ZERO else durationVisitor.visit(ctx.offset())
            val ident = ctx.identifier()?.text ?: ""
            return if (ctx.range() == null) {
                InstantSelector(ident, matches ?: listOf(), offset)
            } else {
                val range = durationVisitor.visit(ctx.range().duration())
                MatrixSelector(ident, matches ?: listOf(), range, offset)
            }
        }

        override fun visitAtom(ctx: PromQLParser.AtomContext?): Expression {
            val e = ctx!!.selector() ?: ctx.application()
            return if (e != null) {
                visit(e)
            } else {
                literalVisitor.visit(ctx.literals())
            }
        }

        override fun visitApplication(ctx: PromQLParser.ApplicationContext?): Expression {
            val fnName = ctx!!.identifier().text.toLowerCase()
            val fnkey = fnName.toLowerCase()
            val aggrGroup = ctx.aggregateGroup()
            val exprs = ctx.exprList()?.expr()?.map { visit(it)!! } ?: listOf()
            if (aggrGroup != null) {
                val aggr = binding.aggregators[fnkey] ?: throw PromQLException("Aggregator $fnName is undefined")
                return AggregatorCall(aggr, exprs, aggregatorGroupVisitor.visit(aggrGroup))
            }
            val fn = binding.functions[fnkey]
            val aggr = binding.aggregators[fnkey]
            return when {
                fn != null -> FunctionCall(fn, exprs)
                aggr != null -> AggregatorCall(aggr, exprs, null)
                else -> throw PromQLException("Function or Aggregator $fnName is undefined")
            }
        }
    }

    class ANTLRErrorDetector(private val includeReports: Boolean = false) : ANTLRErrorListener {
        val errors = mutableListOf<String>()
        val reports = mutableListOf<String>()

        fun hasError(): Boolean {
            return !(errors.isEmpty() && (!includeReports || reports.isEmpty()))
        }

        fun errorMessage(): String? {
            return if (!this.hasError()) {
                null
            } else {
                (errors + reports).joinToString()
            }
        }

        override fun reportAttemptingFullContext(recognizer: Parser?, dfa: DFA?, startIndex: Int, stopIndex: Int, conflictingAlts: BitSet?, configs: ATNConfigSet?) {
            reports += "detected AttemptingFullContext"
        }

        override fun syntaxError(recognizer: Recognizer<*, *>?, offendingSymbol: Any?, line: Int, charPositionInLine: Int, msg: String?, e: RecognitionException?) {
            errors += "line $line:$charPositionInLine $msg"
        }

        override fun reportAmbiguity(recognizer: Parser?, dfa: DFA?, startIndex: Int, stopIndex: Int, exact: Boolean, ambigAlts: BitSet?, configs: ATNConfigSet?) {
            reports += "detected Ambiguity"
        }

        override fun reportContextSensitivity(recognizer: Parser?, dfa: DFA?, startIndex: Int, stopIndex: Int, prediction: Int, configs: ATNConfigSet?) {
            reports += "detected ContextSensitivity"
        }
    }

    const val DEFAULT_ANTLR_REPORT = false

    fun parseInstantSelector(input: CharStream, reportANTLRIssue: Boolean = DEFAULT_ANTLR_REPORT): InstantSelector {
        val ast = parse(input, reportANTLRIssue)
        return when (ast) {
            is InstantSelector -> ast
            else -> throw PromQLException("invalid selector")
        }
    }

    fun parseDuration(input: CharStream, reportANTLRIssue: Boolean = DEFAULT_ANTLR_REPORT): Duration {
        val errs = ANTLRErrorDetector(reportANTLRIssue)

        val lexer = PromQLLexer(input)
        lexer.removeErrorListeners()
        lexer.addErrorListener(errs)

        val tokenStream = CommonTokenStream(lexer)
        val parser = PromQLParser(tokenStream)
        parser.removeErrorListeners()
        parser.addErrorListener(errs)
        val res = durationVisitor.visit(parser.duration())
        if (errs.hasError()) {
            throw PromQLException(errs.errorMessage()!!)
        } else {
            return res
        }
    }

    fun parse(input: CharStream, reportANTLRIssue: Boolean = DEFAULT_ANTLR_REPORT): Expression? {
        val errs = ANTLRErrorDetector(reportANTLRIssue)

        val lexer = PromQLLexer(input)
        lexer.removeErrorListeners()
        lexer.addErrorListener(errs)

        val tokenStream = CommonTokenStream(lexer)
        val parser = PromQLParser(tokenStream)
        parser.removeErrorListeners()
        parser.addErrorListener(errs)
        val visitor = ExpressionVisitor(Binding.default)
        val res = visitor.visit(parser.expression())
        if (errs.hasError()) {
            throw PromQLException(errs.errorMessage()!!)
        } else {
            return res
        }
    }
}