package epimetheus.engine

import epimetheus.model.Mat
import epimetheus.model.TimeRange
import epimetheus.pkg.promql.MatrixSelector
import epimetheus.pkg.promql.InstantSelector
import epimetheus.storage.IgniteGateway

class Fetch(val storage: IgniteGateway) {
    fun collect(ast: InstantSelector, range: TimeRange): Mat {
        throw NotImplementedError()
    }

    fun collect(ast: MatrixSelector, range: TimeRange): Mat {
        throw NotImplementedError()
    }
}