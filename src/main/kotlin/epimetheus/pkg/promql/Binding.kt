package epimetheus.pkg.promql

class Binding(
        val functions: Map<String, Function>,
        val aggregators: Map<String, Aggregator>,
        val binaryOps: Map<String, BinaryOp>
) {
    companion object {
        val default = Binding(
                Function.builtins.map { it.name to it }.toMap(),
                Aggregator.builtins.map { it.name to it }.toMap(),
                BinaryOp.builtins.map { it.name to it }.toMap()
        )
    }
}