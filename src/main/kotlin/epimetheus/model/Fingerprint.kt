package epimetheus.model

typealias Signature = Long

object FNV64 {
    private val INITIAL_VALUE = -0x340d631b7bdddcdbL
    private val MULTIPLIER = 0x100000001b3L

    fun create(): Signature {
        return INITIAL_VALUE
    }

    fun update(value: Signature, b: Byte): Signature {
        return (value xor (b.toLong())) * MULTIPLIER
    }

    fun update(v: Signature, ba: String): Signature {
        var value = v
        for (b in ba) {
            value = value xor b.toLong()
            value *= MULTIPLIER
        }
        return value
    }

    fun update(v: Signature, ba: ByteArray): Signature {
        var value = v
        for (b in ba) {
            value = value xor b.toLong()
            value *= MULTIPLIER
        }
        return value
    }

    fun update(v: Signature, b: ByteArray, off: Int, len: Int): Signature {
        var value = v
        val stop = off + len
        for (i in off until stop) {
            value = value xor b[i].toLong()
            value *= MULTIPLIER
        }
        return value
    }
}
