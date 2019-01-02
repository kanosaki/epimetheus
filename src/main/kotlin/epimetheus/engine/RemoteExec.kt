package epimetheus.engine

import epimetheus.engine.graph.PlanNode
import epimetheus.engine.plan.RuntimeValue
import epimetheus.engine.primitive.Aggregator
import epimetheus.engine.primitive.Function
import epimetheus.storage.IgniteGateway
import org.apache.ignite.Ignite
import org.apache.ignite.lang.IgniteCallable
import org.apache.ignite.resources.IgniteInstanceResource

class RemoteExec(val node: PlanNode, val ec: ExecContext) : IgniteCallable<RuntimeValue> {
    @IgniteInstanceResource
    lateinit var ignite: Ignite

    override fun call(): RuntimeValue {
        val eng = EngineContext(IgniteGateway(ignite), Aggregator.builtins, Function.builtins, ignite)
        return node.evaluate(ec, eng)
    }
}