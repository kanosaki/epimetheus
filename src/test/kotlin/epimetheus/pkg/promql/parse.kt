package epimetheus.pkg.promql

import org.antlr.v4.runtime.CharStreams
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals

class ParseTest {
    // utility functions to construct AST
    private fun bin(opName: String, lhs: Expression, rhs: Expression): Expression {
        return BinaryCall(op(opName), lhs, rhs, VectorMatching(VectorMatchingCardinality.OneToOne, listOf(), false, listOf()))
    }

    private fun op(name: String): BinaryOp {
        return BinaryOp.builtins.first { it.name == name }
    }

    private fun num(n: Double): NumberLiteral {
        return NumberLiteral(n)
    }

    private fun str(s: String): StringLiteral {
        return StringLiteral(s)
    }

    private fun vec(n: String, vararg matches: LabelMatch): InstantSelector {
        return InstantSelector(n, matches.toList(), Duration.ZERO)
    }

    @Test
    fun testBasic() {
        val bind = Binding.default
        val data = listOf(
                "1" to
                        num(1.0), // TODO: add test for literals

                "1 + 2" to
                        bin("+", num(1.0), num(2.0)),

                "1 + 2 + 3" to
                        bin("+", bin("+", num(1.0), num(2.0)), num(3.0)),

                "1 + 2 * 3" to
                        bin("+", num(1.0), bin("*", num(2.0), num(3.0))),

                "(1 + 2) * 3" to
                        bin("*", bin("+", num(1.0), num(2.0)), num(3.0)),

                "foo{hoge=\"fuga\", a=~\"b.*\", c!=1} offset 10s" to
                        InstantSelector(
                                "foo",
                                listOf(LabelMatch("hoge", "=", str("fuga")),
                                        LabelMatch("a", "=~", str("b.*")),
                                        LabelMatch("c", "!=", num(1.0))
                                ), Duration.ofSeconds(10)),

                "foo{a!~\"b.*\"}[2h] offset 1d" to
                        MatrixSelector(
                                "foo",
                                listOf(
                                        LabelMatch("a", "!~", str("b.*"))
                                ), Duration.ofHours(2), Duration.ofDays(1)),

                "foo + bar" to
                        bin("+", vec("foo"), vec("bar")),

                "foo + on (b) group_right(c) bar" to
                        BinaryCall(
                                op("+"),
                                vec("foo"),
                                vec("bar"),
                                VectorMatching(VectorMatchingCardinality.OneToMany, listOf("b"), true, listOf("c"))
                        ),

                "foo + on (a) bar * ignoring(b) baz" to
                        BinaryCall(
                                op("+"),
                                vec("foo"),
                                BinaryCall(
                                        op("*"),
                                        vec("bar"),
                                        vec("baz"),
                                        VectorMatching(VectorMatchingCardinality.OneToOne, listOf("b"), false, listOf())),
                                VectorMatching(VectorMatchingCardinality.OneToOne, listOf("a"), true, listOf())),
                "rate(http_requests{group=~\"pro.*\"}[1m])" to
                        FunctionCall(
                                Function.builtins.first { it.name == "rate" },
                                listOf(
                                        MatrixSelector("http_requests", listOf(
                                                LabelMatch("group", "=~", str("pro.*"))
                                        ), Duration.ofMinutes(1), Duration.ZERO)
                                )
                        ),
                "sum(foo + bar)" to
                        AggregatorCall(
                                bind.aggregators["sum"]!!,
                                listOf(
                                        bin("+", vec("foo"), vec("bar"))
                                ), null),

                "avg without (a, b) (foo)" to
                        AggregatorCall(
                                bind.aggregators["avg"]!!,
                                listOf(
                                        vec("foo")
                                ), AggregatorGroup(AggregatorGroupType.Without, listOf("a", "b"))),

                "avg(foo) by (a, b) " to
                        AggregatorCall(
                                bind.aggregators["avg"]!!,
                                listOf(
                                        vec("foo")
                                ), AggregatorGroup(AggregatorGroupType.By, listOf("a", "b"))),
                "node_cpu / on (instance) group_left sum by (instance,job)(node_cpu)" to
                        BinaryCall(
                                bind.binaryOps["/"]!!,
                                vec("node_cpu"),
                                AggregatorCall(
                                        bind.aggregators["sum"]!!,
                                        listOf(vec("node_cpu")),
                                        AggregatorGroup(AggregatorGroupType.By, listOf("instance", "job"))
                                ),
                                VectorMatching(VectorMatchingCardinality.ManyToOne, listOf("instance"), true, listOf())
                        )
        )
        data.forEachIndexed { index, pair ->
            assertEquals(pair.second, PromQL.parse(CharStreams.fromString(pair.first)), "Error at $index : ${pair.first}")
        }
    }

    @Test
    fun testParseDuration() {
        listOf(
                "5h" to Duration.ofHours(5),
                "5m" to Duration.ofMinutes(5),
                "5s" to Duration.ofSeconds(5)
        ).forEach {
            assertEquals(it.second, PromQL.parseDuration(CharStreams.fromString(it.first)))
        }
    }
}