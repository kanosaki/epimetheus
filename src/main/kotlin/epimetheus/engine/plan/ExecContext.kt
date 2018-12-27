package epimetheus.engine.plan

import epimetheus.storage.Gateway
import org.apache.ignite.Ignite

class ExecContext(val ignite: Ignite, val storage: Gateway) {

}