package epimetheus.engine

import epimetheus.engine.graph.PlanNode
import epimetheus.engine.plan.RuntimeValue
import epimetheus.engine.primitive.Aggregator
import epimetheus.engine.primitive.Function
import epimetheus.storage.Gateway
import org.apache.ignite.Ignite

class EngineContext(
        val gateway: Gateway,
        val aggregators: Map<String, Aggregator>,
        val functions: Map<String, Function>,
        val ignite: Ignite?) {


    companion object {
        fun builtin(gateway: Gateway, ignite: Ignite?): EngineContext {
            return EngineContext(gateway, Aggregator.builtins, Function.builtins, ignite)
        }
    }
}
