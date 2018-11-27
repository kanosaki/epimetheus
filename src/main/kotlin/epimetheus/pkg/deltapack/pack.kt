package epimetheus.pkg.deltapack

import java.io.Serializable

class DoubleDeltaPack(val init: Double, val data: ByteArray) : Serializable {
    companion object {
        fun pack(vs: DoubleArray): DoubleDeltaPack {
            TODO()
        }
    }
}

class LongDeltaDeltaPack(val initValue: Double, val firstDelta: Long, val data: ByteArray) {
    companion object {
        fun pack(vs: DoubleArray): LongDeltaDeltaPack {
            TODO()
        }
    }
}

