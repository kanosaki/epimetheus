package epimetheus.pkg.promql

import epimetheus.DurationUtil.toPromString
import epimetheus.model.LabelMatchType
import epimetheus.model.LabelMatcher
import epimetheus.model.Metric
import epimetheus.model.MetricMatcher
import java.time.Duration

interface Expression {
    fun children(): List<Expression> {
        return listOf()
    }
}

enum class ValueType {
    Vector, Matrix, Scalar, String
}

enum class AggregatorGroupType {
    Without, By
}

enum class LabelMatchOptionType {
    On, Ignoring
}

enum class LabelGroupOptionType {
    Left, Right
}

data class LabelMatch(val name: String, val op: String, val rhs: Literal) {
    fun toMatcher(): LabelMatcher {
        val lmt = when (op) {
            "=" -> LabelMatchType.Eq
            "!=" -> LabelMatchType.Neq
            "=~" -> LabelMatchType.Match
            "!~" -> LabelMatchType.NotMatch
            else -> {
                throw PromQLException("Unknown label match operator: $op")
            }
        }
        return LabelMatcher(lmt, rhs.labelMatchExpr())
    }
}

interface Selector : Expression {
    val name: String
    val labels: List<LabelMatch>
    val offset: Duration
}

abstract class SelectorBase : Selector {
    val matcher: MetricMatcher by lazy {
        if (name == "" || labels.any { it.name == Metric.nameLabel }) {
            MetricMatcher(listOf(
                    *labels.map { it.name to it.toMatcher() }.toTypedArray()
            ))
        } else {
            MetricMatcher(listOf(
                    Metric.nameLabel to LabelMatcher(LabelMatchType.Match, name),
                    *labels.map { it.name to it.toMatcher() }.toTypedArray()
            ))
        }
    }

    fun selectorTostring(): String {
        val lms = labels.joinToString(",") { "${it.name}${it.op}${it.rhs}" }
        return when {
            name == "" -> "{$lms}"
            lms.isEmpty() -> name
            else -> "$name{$lms}"
        }
    }
}

data class InstantSelector(override val name: String, override val labels: List<LabelMatch>, override val offset: Duration) : SelectorBase() {
    override fun toString(): String {
        return if (offset.isZero) {
            this.selectorTostring()
        } else {
            this.selectorTostring() + " offset ${offset.toPromString()}"
        }
    }
}

data class MatrixSelector(override val name: String, override val labels: List<LabelMatch>, val range: Duration, override val offset: Duration) : SelectorBase() {
    override fun toString(): String {
        return if (offset.isZero) {
            this.selectorTostring() + "[${range.toPromString()}]"
        } else {
            this.selectorTostring() + "[${range.toPromString()}] offset ${offset.toPromString()}"
        }
    }
}

data class BinaryCall(val op: BinaryOp, val lhs: Expression, val rhs: Expression, val matching: VectorMatching) : Expression {
    override fun children(): List<Expression> {
        return listOf(lhs, rhs)
    }

    override fun toString(): String {
        return "$lhs ${op.name} $rhs"
    }
}

data class FunctionCall(val fn: Function, val args: List<Expression>) : Expression {
    init {
        // TODO: Add argument validation
    }

    override fun toString(): String {
        return "${fn.name}(${args.joinToString(",")})"
    }

    override fun children(): List<Expression> {
        return args
    }
}

data class AggregatorGroup(val typ: AggregatorGroupType, val labels: List<String>) {
    override fun toString(): String {
        return "${typ.name}(${labels.joinToString(",")})"
    }
}

enum class VectorMatchingCardinality {
    OneToOne,
    /**
     * group_left
     */
    ManyToOne,
    /**
     * group_right
     */
    OneToMany,
    ManyToMany,
}

data class VectorMatching(
        val card: VectorMatchingCardinality,
        val matchingLabels: List<String>,
        /**
         * true -> match on
         * false -> match ignoring
         */
        val on: Boolean,
        val include: List<String>
)

data class LabelMatchOption(val mode: LabelMatchOptionType, val labels: List<String>)
data class LabelGroupOption(val mode: LabelGroupOptionType, val labels: List<String>)

data class AggregatorCall(val agg: Aggregator, val params: List<Expression>, val groups: AggregatorGroup?) : Expression {
    init {

    }

    override fun toString(): String {
        return "${agg.name}(${params.joinToString(",")})($groups)"
    }

    override fun children(): List<Expression> {
        return params
    }
}

interface Literal : Expression {
    fun labelMatchExpr(): String
}

data class StringLiteral(val value: String) : Literal {
    override fun labelMatchExpr(): String {
        return value
    }

    override fun toString(): String {
        return "\"$value\""
    }
}

data class NumberLiteral(val value: Double) : Literal {
    override fun labelMatchExpr(): String {
        return value.toString()
    }

    override fun toString(): String {
        return value.toString()
    }
}

object Ast {
    fun traverse(ast: Expression, callback: (Expression) -> Unit) {
        callback(ast)
        for (c in ast.children()) {
            traverse(c, callback)
        }
    }
}

data class BoolConvert(val expr: Expression) : Expression

enum class VisitNext {
    Continue, Stop
}

class AstVisitor(val callback: (Expression) -> VisitNext) {

    private fun visit0(ast: Expression, recurse: Boolean): Boolean {
        val cont = callback(ast)
        when (cont) {
            VisitNext.Continue -> {
                val children = ast.children()
                for (c in children) {
                    if (!visit0(c, true)) {
                        return false
                    }
                }
                return true
            }
            VisitNext.Stop -> return false
        }
    }

    fun visit(ast: Expression) {
        visit0(ast, false)
    }
}
