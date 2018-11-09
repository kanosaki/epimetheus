package epimetheus.model

object SpecialValue {

    fun nanPack(data: Long): Double {
        return Double.fromBits(Double.NaN.toRawBits() or data)
    }

    val STALE_VALUE = nanPack(1 shl 50)
}