package epimetheus.model

import java.io.Externalizable
import java.io.ObjectInput
import java.io.ObjectOutput


class DoubleSlice(val values: DoubleArray, val begin: Int, override val size: Int) : List<Double>, Externalizable {
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

    override fun readExternal(inp: ObjectInput?) {
        TODO()
    }

    override fun writeExternal(out: ObjectOutput?) {
        TODO()
    }

    override fun contains(element: Double): Boolean {
        for (i in begin..(begin + size)) {
            if (values[i] == element) {
                return true
            }
        }
        return false
    }

    override fun containsAll(elements: Collection<Double>): Boolean {
        TODO("not implemented")
    }

    override fun get(index: Int): Double {
        if (index >= size) {
            throw IndexOutOfBoundsException(index.toString())
        }
        return values[begin + index]
    }

    override fun indexOf(element: Double): Int {
        TODO("not implemented")
    }

    override fun isEmpty(): Boolean {
        return size == 0
    }

    override fun iterator(): Iterator<Double> {
        return SliceIterator(values, begin, size)
    }

    override fun lastIndexOf(element: Double): Int {
        TODO("not implemented")
    }

    override fun listIterator(): ListIterator<Double> {
        return SliceIterator(values, begin, size)
    }

    override fun listIterator(index: Int): ListIterator<Double> {
        return SliceIterator(values, begin + index, size - index)
    }

    override fun subList(fromIndex: Int, toIndex: Int): List<Double> {
        TODO("not implemented")
    }

    class SliceIterator(val values: DoubleArray, val begin: Int, val size: Int) : ListIterator<Double> {
        var ptr = 0
        override fun hasNext(): Boolean {
            return ptr < size
        }

        override fun hasPrevious(): Boolean {
            return ptr > 0
        }

        override fun next(): Double {
            val p = begin + ptr++
            return values[p]
        }

        override fun nextIndex(): Int {
            return ptr
        }

        override fun previous(): Double {
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
}

class LongSlice(val values: LongArray, val begin: Int, override val size: Int) : List<Long> {
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
}
