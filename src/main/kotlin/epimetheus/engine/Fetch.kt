package epimetheus.engine

import epimetheus.model.GridMat
import epimetheus.model.TimeRange
import epimetheus.pkg.promql.InstantSelector
import epimetheus.pkg.promql.MatrixSelector
import epimetheus.storage.IgniteGateway

class Fetch(val storage: IgniteGateway) {
    fun collect(ast: InstantSelector, range: TimeRange): GridMat {
        throw NotImplementedError()
    }

    fun collect(ast: MatrixSelector, range: TimeRange): GridMat {
        throw NotImplementedError()
    }
}