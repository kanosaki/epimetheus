package epimetheus.engine.plan

object ValueUtils {
    fun boolConvert(v: Double): Double {
        return if (v == 0.0 || !v.isFinite()) {
            0.0
        } else {
            1.0
        }
    }
}

