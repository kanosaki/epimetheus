package epimetheus.engine

import epimetheus.model.TimeFrames

data class ExecContext(val frames: TimeFrames, val tracer: Tracer)
