package epimetheus.model


data class TimeRange(val from: Long, val to: Long) {
    fun contains(ts: Long): Boolean {
        return ts in from..to
    }
}

// start == end --> instant selector
data class TimeFrames(val start: Long, val end: Long, val step: Long) : List<Long> {
    companion object {
        fun instant(ts: Long): TimeFrames {
            return TimeFrames(ts, ts, 1)
        }
    }

    init {
        assert(start <= end)
        if (start != end) {
            assert(step > 0)
        }
    }

    override fun toString(): String {
        return when {
            step > 1L -> "[$start→$end,$step]"
            step == 1L -> "[$start→$end]"
            step == 0L -> "[$start]"
            else -> throw RuntimeException("never here")
        }
    }

    fun stretchStart(t: Long, inclusive: Boolean = true): TimeFrames {
        if (t == 0L) {
            return this
        }
        val jitter = t % step
        return if (inclusive) {
            this.copy(start = start - t - (step - jitter), end = end, step = step)
        } else {
            this.copy(start = start - t + jitter, end = end, step = step)
        }
    }

    override fun get(index: Int): Long {
        if (index > size) {
            throw IndexOutOfBoundsException("$index exceeds bounds")
        } else {
            return start + step * index
        }
    }

    override fun indexOf(element: Long): Int {
        return if (contains(element)) {
            ((element - start) / step).toInt()
        } else {
            -1
        }
    }

    override fun lastIndexOf(element: Long): Int {
        return indexOf(element)
    }

    override fun listIterator(): ListIterator<Long> {
        return FrameIterator(this)
    }

    override fun listIterator(index: Int): ListIterator<Long> {
        return FrameIterator(this, index)
    }

    override fun subList(fromIndex: Int, toIndex: Int): List<Long> {
        return TimeFrames(start + step * fromIndex, end - step * (size - toIndex - 1), step)
    }

    override val size: Int
        get() = ((end - start) / step).toInt() + 1

    override fun contains(element: Long): Boolean {
        return element in start..end && (element - start) % step == 0L
    }

    fun includes(element: Long): Boolean {
        return element in start..end
    }

    override fun containsAll(elements: Collection<Long>): Boolean {
        return elements.all { contains(it) }
    }

    override fun isEmpty(): Boolean {
        return false
    }

    override fun iterator(): Iterator<Long> {
        return FrameIterator(this)
    }

    fun toLongArray(): LongArray {
        return LongArray(this.size) { this[it] }
    }

    class FrameIterator(val frame: TimeFrames, begin: Int = 0) : ListIterator<Long> {
        var index = begin

        override fun hasPrevious(): Boolean {
            return index > 0
        }

        override fun nextIndex(): Int {
            return index + 1
        }

        override fun previous(): Long {
            return frame[index--]
        }

        override fun previousIndex(): Int {
            return index - 1
        }

        override fun hasNext(): Boolean {
            return index < frame.size
        }

        override fun next(): Long {
            return frame[index++]
        }
    }
}