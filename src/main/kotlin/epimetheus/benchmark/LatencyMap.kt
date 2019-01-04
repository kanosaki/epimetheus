package epimetheus.benchmark

import java.util.concurrent.ConcurrentLinkedDeque

class LatencyMap(val columns: List<String>) {
    private val colmapper = columns.mapIndexed { i, c -> c to i }.toMap()
    private val rows = ConcurrentLinkedDeque<DoubleArray>()

    fun commit(writer: Writer) {
        rows += writer.row()
    }

    fun col(name: String): List<Double> {
        val colIdx = colmapper[name]!!
        return rows.map { it[colIdx] }
    }

    fun newWriter(): Writer {
        return Writer(this)
    }

    val size: Int
        get() = rows.size

    class Writer(val m: LatencyMap) {
        private var currentRow: DoubleArray = DoubleArray(m.columns.size)

        operator fun set(col: String, value: Double) {
            val colIdx = m.colmapper[col]
            if (colIdx != null) {
                currentRow[colIdx] = value
            }
        }

        fun row(): DoubleArray {
            return currentRow
        }

        fun commit() {
            m.commit(this)
        }

        companion object {
            val Void = Writer(LatencyMap(listOf()))
        }
    }
}