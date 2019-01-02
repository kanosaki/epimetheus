package epimetheus.model


data class DoubleSlice(val values: DoubleArray, val begin: Int, val size: Int) {
    companion object {
        fun wrap(a: DoubleArray): DoubleSlice {
            return DoubleSlice(a, 0, a.size)
        }

        inline fun init(size: Int, fn: (Int) -> Double): DoubleSlice {
            return wrap(DoubleArray(size) { fn(it) })
        }
    }

    fun write(i: Int, v: Double) {
        values[begin + i] = v
    }

    fun clone(): DoubleSlice {
        return DoubleSlice(values.copyOfRange(begin, size), 0, size)
    }

    inline fun mapCopy(fn: (Int, Double) -> Double): DoubleSlice {
        val mapped = DoubleArray(size) {
            fn(it, values[begin + it])
        }
        return DoubleSlice(mapped, 0, size)
    }

    inline fun mapInplace(fn: (Int, Double) -> Double) {
        for (i in 0 until size) {
            values[begin + i] = fn(i, values[begin + i])
        }
    }

    fun contains(element: Double): Boolean {
        for (i in begin until (begin + size)) {
            if (values[i] == element) {
                return true
            }
        }
        return false
    }

    fun isAllStale(): Boolean {
        for (i in begin until (begin + size)) {
            if (!Mat.isStale(values[i])) {
                return false
            }
        }
        return true
    }

    fun toList(): List<Double> {
        val ret = mutableListOf<Double>()
        for (i in begin until (begin + size)) {
            ret += values[i]
        }
        return ret
    }

    fun last(): Double {
        if (size == 0) {
            throw NoSuchElementException()
        }
        return values[begin + size - 1]
    }

    fun first(): Double {
        if (size == 0) {
            throw NoSuchElementException()
        }
        return values[begin]
    }

    inline fun count(pred: (Double) -> Boolean): Int {
        var ret = 0
        for (i in begin until (begin + size)) {
            if (pred(values[i])) {
                ret++
            }
        }
        return ret
    }

    fun sum(): Double {
        var ret = 0.0
        for (i in begin until (begin + size)) {
            ret += values[i]
        }
        return ret
    }

    operator fun get(index: Int): Double {
        if (index >= size) {
            throw IndexOutOfBoundsException(index.toString())
        }
        return values[begin + index]
    }

    fun isEmpty(): Boolean {
        return size == 0
    }

    operator fun iterator(): DoubleIterator {
        return SliceIterator(values, begin, size)
    }

    class SliceIterator(val values: DoubleArray, val begin: Int, val size: Int) : DoubleIterator() {
        override fun nextDouble(): Double {
            val p = begin + ptr++
            return values[p]
        }

        var ptr = 0
        override fun hasNext(): Boolean {
            return ptr < size
        }
    }

//    override fun toString(): String {
//        return this.toList().toString()
//    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DoubleSlice

        if (size != other.size) {
            return false
        }
        for (i in 0 until size) {
            if (values[i] != other.get(i)) {
                return false
            }
        }
        return true
    }

    override fun hashCode(): Int {
        // from Arrays.hashCode
        var result = 1
        for (i in 0 until size) {
            val v = values[i].toRawBits()
            val elementHash = (v xor v.ushr(32)).toInt()
            result = 31 * result + elementHash
        }

        result = 31 * result + begin
        result = 31 * result + size
        return result
    }
}

data class LongSlice(val values: LongArray, val begin: Int, override val size: Int) : List<Long> {
    companion object {
        fun wrap(a: LongArray): LongSlice {
            return LongSlice(a, 0, a.size)
        }
    }

    override fun containsAll(elements: Collection<Long>): Boolean {
        TODO("not implemented")
    }

    override fun get(index: Int): Long {
        if (index >= size) {
            throw IndexOutOfBoundsException(index.toString())
        }
        return values[begin + index]
    }

    override fun indexOf(element: Long): Int {
        TODO("not implemented")
    }

    override fun isEmpty(): Boolean {
        return size == 0
    }

    override fun iterator(): Iterator<Long> {
        return SliceIterator(values, begin, size)
    }

    override fun lastIndexOf(element: Long): Int {
        TODO("not implemented")
    }

    override fun listIterator(): ListIterator<Long> {
        return SliceIterator(values, begin, size)
    }

    override fun listIterator(index: Int): ListIterator<Long> {
        return SliceIterator(values, begin + index, size - index)
    }

    override fun subList(fromIndex: Int, toIndex: Int): List<Long> {
        TODO("not implemented")
    }

    override fun contains(element: Long): Boolean {
        TODO("not implemented")
    }

    class SliceIterator(val values: LongArray, val begin: Int, val size: Int) : ListIterator<Long> {
        var ptr = 0
        override fun hasNext(): Boolean {
            return ptr < size
        }

        override fun hasPrevious(): Boolean {
            return ptr > 0
        }

        override fun next(): Long {
            val p = begin + ptr++
            return values[p]
        }

        override fun nextIndex(): Int {
            return ptr
        }

        override fun previous(): Long {
            val p = begin + --ptr
            return values[p]
        }

        override fun previousIndex(): Int {
            return ptr - 1
        }
    }

    override fun toString(): String {
        return this.toList().toString()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as LongSlice

        if (size != other.size) {
            return false
        }
        for (i in 0 until size) {
            if (values[i] != other[i]) {
                return false
            }
        }
        return true
    }

    override fun hashCode(): Int {
        // from Arrays.hashCode
        var result = 1
        for (i in 0 until size) {
            val elementHash = (values[i] xor values[i].ushr(32)).toInt()
            result = 31 * result + elementHash
        }

        result = 31 * result + begin
        result = 31 * result + size
        return result
    }
}
