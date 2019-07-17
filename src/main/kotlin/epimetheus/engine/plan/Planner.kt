package epimetheus.engine.plan

import epimetheus.engine.EngineContext
import epimetheus.engine.graph.*
import epimetheus.engine.primitive.Aggregator
import epimetheus.engine.primitive.BOp
import epimetheus.engine.primitive.Function
import epimetheus.model.*
import epimetheus.pkg.promql.*
import epimetheus.storage.Gateway
import tech.tablesaw.api.StringColumn
import tech.tablesaw.api.Table
import tech.tablesaw.columns.Column
import java.util.*
import kotlin.Comparator

class Planner(
        val storage: Gateway,
        val binOpPlanner: BinOpPlanner = BinOpPlanner(BOp.builtinMap),
        val functionPlanner: FunctionPlanner = FunctionPlanner(Function.builtins),
        val aggregatorPlanner: AggregatorPlanner = AggregatorPlanner(Aggregator.builtins)) {

    fun plan(ast: Expression, ctx: EngineContext): PlanNode {
        return when (ast) {
            is NumberLiteral -> {
                ScalarLiteralNode(ast.value)
            }
            is StringLiteral -> {
                StringLiteralNode(ast.value)
            }
            is InstantSelector -> {
                planInstant(ast, ctx)
            }
            is MatrixSelector -> {
                planMatrix(ast, ctx)
            }
            is BinaryCall -> {
                binOpPlanner.plan(this, ast, ctx)
            }
            is MinusExpr -> {
                binOpPlanner.plan(this,
                        BinaryCall(
                                BinaryOp.mulOp,
                                NumberLiteral(-1.0),
                                ast.expr,
                                VectorMatching(VectorMatchingCardinality.OneToOne, listOf(), false, listOf())),
                        ctx)
            }
            is AggregatorCall -> {
                aggregatorPlanner.plan(this, ast, ctx)
            }
            is FunctionCall -> {
                functionPlanner.plan(this, ast, ctx)
            }
            is BoolConvert -> {
                val v = plan(ast.expr, ctx)
                return when (v) {
                    is InstantNode -> BoolConvertNode(v.metPlan, v)
                    is ScalarLiteralNode -> ScalarLiteralNode(ValueUtils.boolConvert(v.value))
                    else -> throw PromQLException("cannot convert ${v.javaClass} to bool")
                }
            }
            else -> {
                TODO("$ast not implemented")
            }
        }
    }

    fun planInstant(ast: InstantSelector, ctx: EngineContext): MergePointNode {
        val mets = storage.metricRegistry.lookupMetrics(ast.matcher)
        return MergePointNode(mets.map { InstantSelectorNode(ast, it, ast.offset) }, ast)
    }

    fun planMatrix(ast: MatrixSelector, ctx: EngineContext): MergeRangeNode {
        val mets = storage.metricRegistry.lookupMetrics(ast.matcher)
        return MergeRangeNode(mets.map { MatrixSelectorNode(it, ast.range, ast.offset) }, ast.range.toMillis())
    }
}

interface MetricPlan

data class FixedMetric(val metrics: List<Metric>) : MetricPlan

object VariableMetric : MetricPlan

