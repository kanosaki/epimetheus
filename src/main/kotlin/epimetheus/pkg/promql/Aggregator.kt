package epimetheus.pkg.promql

import epimetheus.model.GridMat
import epimetheus.model.Value

data class Aggregator(
        val name: String,
        override val argTypes: List<ValueType> = listOf(ValueType.Matrix),
        override val returnType: ValueType = ValueType.Vector,
        override val varidaric: Int = 0,
        val evalFn: (params: List<Value>, groups: AggregatorGroup?) -> GridMat
) : Applicative {
    companion object {
        val builtins = listOf(
                Aggregator("sum") { p, g -> TODO() },
                Aggregator("avg") { p, g -> TODO() },
                Aggregator("count") { p, g -> TODO() },
                Aggregator("min") { p, g -> TODO() },
                Aggregator("max") { p, g -> TODO() },
                Aggregator("stddev") { p, g -> TODO() },
                Aggregator("stdvar") { p, g -> TODO() },
                Aggregator("topk") { p, g -> TODO() },
                Aggregator("bottomk") { p, g -> TODO() },
                Aggregator("count_values") { p, g -> TODO() },
                Aggregator("quantile") { p, g -> TODO() }
        )
    }
}
