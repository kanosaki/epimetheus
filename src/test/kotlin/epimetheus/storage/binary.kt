package epimetheus.storage

import org.apache.ignite.Ignition
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.configuration.IgniteConfiguration
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import tech.tablesaw.api.DoubleColumn
import tech.tablesaw.api.Table
import java.util.*

class EdenPageBinaryTest {

    fun extractData(bo: BinaryObject): ByteArray {
        val f = bo.javaClass.getDeclaredField("arr")
        f.isAccessible = true
        return f.get(bo) as ByteArray
    }

    @Tag("slow")
    @Test
    fun compressionTest() {
        val ignite = Ignition.getOrStart(IgniteConfiguration())
        val rand = Random()

        for (dynamicRange in listOf(1.0, 10.0, 100.0)) {
            fun linear(i: Int): Long {
                return i.toLong() * 15 * 1000
            }

            fun linearUniform(i: Int): Long {
                return i.toLong() * 15 * 1000 + rand.nextInt(50 * dynamicRange.toInt()) - 25 * dynamicRange.toInt()
            }

            fun linearGaussian(i: Int): Long {
                return i.toLong() * 15 * 1000 + (rand.nextGaussian() * 25 * dynamicRange).toLong()
            }

            fun vGaussian(v: Int): Double {
                return v * 100 + rand.nextGaussian() * dynamicRange
            }

            fun vUniform(v: Int): Double {
                return v * 100 + rand.nextDouble() * dynamicRange
            }


            fun vLinear(v: Int): Double {
                return v * 100.0
            }

            fun vConst(v: Int): Double {
                return 100.0
            }

            val generators = listOf(
                    Triple(::linear, ::vConst, "none/const"),
                    Triple(::linear, ::vLinear, "none/linear"),
                    Triple(::linear, ::vUniform, "none/uniform"),
                    Triple(::linear, ::vGaussian, "none/gaussian"),
                    Triple(::linearUniform, ::vGaussian, "uniform/gaussian"),
                    Triple(::linearGaussian, ::vGaussian, "gaussian/gaussian")
            )
            val samplesCount = listOf(20, 40, 80, 160, 320, 640) // 5min, 10min, 20min, 40min, 1h, 2h (points in a page for a metric with 15 sec scrape interval)

            val ratioColumns = mutableListOf<DoubleColumn>()
            val sizeColumns = mutableListOf<DoubleColumn>()

            for (gens in generators) {
                val bytesPerPoint = mutableListOf<Double>()
                val sizes = mutableListOf<Double>()

                // Empty data size
                sizes += extractData(ignite.binary().toBinary<BinaryObject>(EdenPage(doubleArrayOf(), longArrayOf()))).size.toDouble()

                for (samples in samplesCount) {
                    val resultSizes = mutableListOf<Int>()
                    for (tryIdx in 0 until 100) {
                        val timestamps = (0 until samples).map(gens.first).toLongArray()
                        val values = (0 until samples).map(gens.second).toDoubleArray()
                        val page = EdenPage(values, timestamps)
                        val bo = ignite.binary().toBinary<BinaryObject>(page)
                        val data = extractData(bo)
                        resultSizes += data.size
                    }
                    sizes += resultSizes.average()
                    bytesPerPoint += (resultSizes.average() / samples)  // Double(8 bytes) + Long(8 bytes)
                }
                ratioColumns += DoubleColumn.create(gens.third, bytesPerPoint.toDoubleArray())
                sizeColumns += DoubleColumn.create(gens.third, sizes.toDoubleArray())
            }
            println("========================= dynamicity $dynamicRange ======================================")
            val ratioTable = Table.create("compression ratio", *ratioColumns.toTypedArray())
            println(ratioTable.printAll())
            println() // spacing
            val sizeTable = Table.create("sizes", *sizeColumns.toTypedArray())
            println(sizeTable.printAll())
            println() // spacing
            println() // spacing
        }
    }
}