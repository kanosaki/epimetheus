package epimetheus.pkg.parquet

import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import java.io.EOFException
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer


class ParquetFile(private val path: String) : InputFile {
    private val f = File(path)

    override fun getLength(): Long {
        return f.length()
    }

    override fun newStream(): SeekableInputStream {
        return ParquetFileInputStream(RandomAccessFile(path, "rb"))
    }
}

class ParquetFileInputStream(val input: RandomAccessFile) : SeekableInputStream() {
    private var pos = 0L
    override fun readFully(bytes: ByteArray) {
        input.seek(0)
        val read = input.read(bytes)
        pos += read
        if (read != bytes.size) {
            throw EOFException()
        }
    }

    override fun readFully(bytes: ByteArray, start: Int, len: Int) {
        val read = input.read(bytes, start, len)
        pos += read
        if (read != bytes.size) {
            throw EOFException()
        }
    }

    override fun readFully(buf: ByteBuffer) {
        val remaining = buf.remaining()
        val read = input.read(buf.array())
        pos += read
        if (read != remaining) {
            throw EOFException()
        }
    }

    override fun read(buf: ByteBuffer?): Int {
        val r = input.read(buf?.array())
        pos += r
        return r
    }

    override fun read(): Int {
        val r = input.read()
        pos++
        return r
    }

    override fun getPos(): Long {
        return pos
    }

    override fun seek(newPos: Long) {
        input.seek(newPos)
        pos = newPos
    }

}