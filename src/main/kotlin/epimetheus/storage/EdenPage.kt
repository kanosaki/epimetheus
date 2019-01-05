package epimetheus.storage

import fi.iki.yak.ts.compression.gorilla.*
import fi.iki.yak.ts.compression.gorilla.predictors.DifferentialFCM
import org.apache.ignite.binary.BinaryReader
import org.apache.ignite.binary.BinaryWriter
import org.apache.ignite.binary.Binarylizable

class EdenPage(var values: DoubleArray, var timestamps: LongArray) : Binarylizable {
    object FieldKeys {
        val DATA = "data"
        val SIZE = "size"
    }

    override fun readBinary(reader: BinaryReader?) {
        val size = reader!!.readShort(FieldKeys.SIZE).toInt()
        val ba = reader.readLongArray(FieldKeys.DATA) ?: return // empty data
        val decompressor = GorillaDecompressor(LongArrayInput(ba))
        values = DoubleArray(size)
        timestamps = LongArray(size)
        for (i in 0 until size) {
            val pair = decompressor.readPair()
                    ?: throw RuntimeException("size is inconsistent: given = $size but null at $i")
            values[i] = pair.doubleValue
            timestamps[i] = pair.timestamp
        }
    }

    override fun writeBinary(writer: BinaryWriter?) {
        val size = values.size + dirtryPairs.size
        writer!!.writeShort(FieldKeys.SIZE, size.toShort())
        if (timestamps.isEmpty() && dirtryPairs.size == 0) {
            writer.writeLongArray(FieldKeys.DATA, null)
            return
        }
        val firstTs = if (timestamps.isNotEmpty()) timestamps[0] else dirtryPairs[0].first

        val out = LongArrayOutput(values.size + dirtryPairs.size)
        val compressor = GorillaCompressor(firstTs, out)
        for (i in 0 until values.size) {
            compressor.addValue(timestamps[i], values[i])
        }
        for (dp in dirtryPairs) {
            compressor.addValue(dp.first, dp.second)
        }
        compressor.close()
        writer.writeLongArray(FieldKeys.DATA, out.longArray)
    }

    constructor() : this(doubleArrayOf(), longArrayOf())

    private val dirtryPairs = mutableListOf<kotlin.Pair<Long, Double>>()

    fun dirtyWrite(ts: Long, v: Double) {
        dirtryPairs += ts to v
    }
}

