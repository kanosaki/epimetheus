package epimetheus.model

import net.jpountz.xxhash.XXHashFactory
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.impl.GenericObjectPool
import java.nio.ByteBuffer
import java.util.*
import kotlin.collections.ArrayList


class UndefinedMetricIDException(metricId: Long) : RuntimeException("MetricID: $metricId is not registered")
interface MetricRegistry {
    fun metric(metricId: Long): Metric?
    fun mustMetric(metricId: Long): Metric {
        return metric(metricId) ?: throw UndefinedMetricIDException(metricId)
    }

    fun lookupMetrics(query: MetricMatcher): List<Metric>
}

class MetricBuilder(val labels: MutableList<Array<String>> = mutableListOf()) : Metric(labels) {
    private fun searchKey(key: String): Int {
        return Collections.binarySearch(labels, key) { o1, o2 ->
            val l1 = o1 as Array<*>
            val k = o2 as String
            (l1[0] as String).compareTo(k)
        }
    }

    fun putOrRemove(key: String, value: String?) {
        if (value == null) {
            remove(key)
        } else {
            put(key, value)
        }
    }

    fun put(key: String, value: String) {
        val idx = searchKey(key)
        if (idx < 0) {
            labels.add(idx.inv(), arrayOf(key, value))
        } else {
            labels[idx] = arrayOf(key, value)
        }
    }

    fun remove(key: String) {
        val idx = searchKey(key)
        if (idx >= 0) {
            labels.removeAt(idx)
        }
    }

    inline fun removeIf(crossinline fn: (String, String) -> Boolean) {
        labels.removeIf {
            fn(it[0], it[1])
        }
    }

    fun build(): Metric {
        labels.sortBy { it[0] }
        return Metric(labels)
    }
}

open class Metric(val lbls: List<Array<String>>) {
    private val fp = computeFingerprint()

    fun fingerprint(): Signature {
        return fp
    }

    private fun computeFingerprint(): Signature {
        var res = FNV64.create()
        for (i in 0 until lbls.size) {
            val k = lbls[i][0]
            val v = lbls[i][1]
            var sum = FNV64.create()
            sum = FNV64.update(sum, k)
            sum = FNV64.update(sum, SeparatorByte)
            sum = FNV64.update(sum, v)
            sum = FNV64.update(sum, SeparatorByte)
            res = res xor sum
        }
        return res
    }

    private fun searchKey(key: String): Int {
        return Collections.binarySearch(lbls, key) { o1, o2 ->
            val l1 = o1 as Array<*>
            (l1[0] as String).compareTo(o2 as String)
        }
    }

    fun get(key: String): String? {
        val idx = searchKey(key)
        return if (idx < 0) {
            null
        } else {
            lbls[idx][1]
        }
    }

    fun builder(): MetricBuilder {
        return MetricBuilder(lbls.toMutableList())
    }

    fun labels(): List<Pair<String, String>> {
        return lbls.map { it[0] to it[1] }
    }

    fun toSortedMap(): SortedMap<String, String> {
        return sortedMapOf(*lbls.map { it[0] to it[1] }.toTypedArray())
    }

    /**
     * on: true -> calc fingerprint only with specified label values (`on` mode)
     * on: false -> calc fingerprint WITHOUT specified label values (`ignoring` mode)
     */
    fun filteredFingerprint(on: Boolean, labels: Collection<String>, withoutNameLabel: Boolean = false): Signature {
        return labelFilteredFingerprintFNV(on, lbls, labels, withoutNameLabel)
    }

    fun name(): String? {
        return get(nameLabel)
    }

    fun filterOn(onLabels: List<String>): Metric {
        return Metric(lbls.filter { onLabels.contains(it[0]) }.toMutableList())
    }

    fun filterWithout(removeName: Boolean, ignoreLabels: List<String>): Metric {
        return Metric(lbls.filter { !ignoreLabels.contains(it[0]) && (!removeName || it[0] != Metric.nameLabel) }.toMutableList())
    }

    override fun toString(): String {
        val labelsExpr = lbls.filter { it[0] != nameLabel }.map { "${it[0]}=\"${it[1]}\"" }.joinToString(",")
        val n = name()
        val nameLabel = when (n) {
            null -> ""
            "" -> "\"\""
            else -> n
        }
        return "$nameLabel{$labelsExpr}"
    }

    private fun valuesEquals(other: Any?): Boolean {
        if (other === null) {
            return false
        }
        if (other === this) {
            return true
        }
        val o = other as? Metric ?: return false
        if (o.lbls.size != this.lbls.size) {
            return false
        }
        for (i in 0 until lbls.size) {
            if (lbls[i][0] != o.lbls[i][0] || lbls[i][1] != o.lbls[i][1]) {
                return false
            }
        }
        return true
    }

    override fun equals(other: Any?): Boolean {
        if (other === null) {
            return false
        }
        if (other === this) {
            return true
        }
        return if (other is Metric) {
            val ret = other.fp == this.fp
            assert(valuesEquals(other) == ret)
            ret
        } else {
            false
        }
    }

    override fun hashCode(): Int {
        return fingerprint().toInt()
    }

    companion object {
        val SeparatorByte: Byte = 0xFF.toByte()
        val nameLabel = "__name__"
        val bucketLabel = "le"
        val instanceLabel = "instance"

        val empty = Metric(mutableListOf())

        val factory = XXHashFactory.fastestInstance()

        val bufpool = GenericObjectPool<ByteBuffer>(object : BasePooledObjectFactory<ByteBuffer>() {
            override fun wrap(obj: ByteBuffer?): PooledObject<ByteBuffer> {
                return DefaultPooledObject<ByteBuffer>(obj)
            }

            override fun passivateObject(p: PooledObject<ByteBuffer>?) {
                p?.`object`?.clear()
            }

            override fun create(): ByteBuffer {
                return ByteBuffer.allocate(1024)
            }
        })

        fun fromSrotedMap(sm: SortedMap<String, String>): Metric {
            return Metric(sm.entries.map { arrayOf(it.key, it.value) })
        }

        fun of(name: String, vararg labels: Pair<String, String>): Metric {
            val al = ArrayList<Array<String>>(labels.size + 1)
            for (i in 0 until labels.size) {
                al.add(arrayOf(labels[i].first, labels[i].second))
            }
            al.add(arrayOf(nameLabel, name))
            al.sortBy { it[0] }
            return Metric(al)
        }

        fun of(vararg labels: Pair<String, String>): Metric {
            val al = ArrayList<Array<String>>(labels.size + 1)
            for (i in 0 until labels.size) {
                al.add(arrayOf(labels[i].first, labels[i].second))
            }
            al.sortBy { it[0] }
            return Metric(al)
        }

        fun labelFilteredFingerprintFNV(on: Boolean, m: List<Array<String>>, onLabels: Collection<String>, excludeNameLabel: Boolean): Signature {
            var res = FNV64.create()
            for (i in 0 until m.size) {
                val k = m[i][0]
                val v = m[i][1]
                if ((!on) xor onLabels.contains(k) && (!excludeNameLabel || k != nameLabel)) {
                    var sum = FNV64.create()
                    sum = FNV64.update(sum, k)
                    sum = FNV64.update(sum, SeparatorByte)
                    sum = FNV64.update(sum, v)
                    sum = FNV64.update(sum, SeparatorByte)
                    res = res xor sum
                }
            }
            return res
        }

        // previous has function (slightly better performance, but it doesn't matter)
        // on my machine: XXHash 1300ns/hash, FNV: 900ns/hash
        fun labelsFingerprintFNV(m: SortedMap<String, String>): Signature {
            var res = FNV64.create()
            m.forEach { k, v ->
                var sum = FNV64.create()
                sum = FNV64.update(sum, k.toByteArray())
                sum = FNV64.update(sum, SeparatorByte)
                sum = FNV64.update(sum, v.toByteArray())
                sum = FNV64.update(sum, SeparatorByte)
                res = res xor sum
            }
            return res
        }

        fun labelFilteredFingerprintXXHash(on: Boolean, m: SortedMap<String, String>, onLabels: Collection<String>): Signature {
            val buf = bufpool.borrowObject(-1)
            m.forEach { k, v ->
                if ((!on) xor onLabels.contains(k)) {
                    for (kc in k) {
                        buf.putChar(kc)
                    }
                    buf.put(SeparatorByte)
                    for (vc in v) {
                        buf.putChar(vc)
                    }
                    buf.put(SeparatorByte)
                }
            }
            buf.rewind()
            val res = factory.hash64().hash(buf, 0) // seed?
            bufpool.returnObject(buf)
            return res
        }

        // current prometheus adopts XXHash
        fun labelsFingerprintXXHash(m: SortedMap<String, String>): Signature {
            val buf = bufpool.borrowObject(-1)
            m.forEach { k, v ->
                for (kc in k) {
                    buf.putChar(kc)
                }
                buf.put(SeparatorByte)
                for (vc in v) {
                    buf.putChar(vc)
                }
                buf.put(SeparatorByte)
            }
            buf.rewind()
            val res = factory.hash64().hash(buf, 0) // seed?
            bufpool.returnObject(buf)
            return res
        }
    }
}
