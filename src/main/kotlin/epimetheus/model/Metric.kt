package epimetheus.model

import net.jpountz.xxhash.XXHashFactory
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.impl.GenericObjectPool
import java.nio.ByteBuffer
import java.util.*


class UndefinedMetricIDException(metricId: Long) : RuntimeException("MetricID: $metricId is not registered")
interface MetricRegistory {
    fun metric(metricId: Long): Metric?
    fun mustMetric(metricId: Long): Metric {
        return metric(metricId) ?: throw UndefinedMetricIDException(metricId)
    }
}

open class Metric(val m: SortedMap<String, String>) {
    fun canonicalizedID(): String {
        return labelsCanonicalizedID(m)
    }

    fun fingerprint(): Signature {
        return labelsFingerprintFNV(m)
    }

    /**
     * on: true -> calc fingerprint only with specified label values (`on` mode)
     * on: false -> calc fingerprint WITHOUT specified label values (`ignoring` mode)
     */
    fun filteredFingerprint(on: Boolean, labels: Collection<String>): Signature {
        return labelFilteredFingerprintFNV(on, m, labels)
    }

    fun name(): String {
        return m[nameLabel]!!
    }

    fun filter(onLabels: Collection<String>, ignoreLabels: Collection<String>): Metric {
        return Metric(m.filter { onLabels.contains(it.key) && !ignoreLabels.contains(it.key) }.toSortedMap())
    }

    override fun toString(): String {
        val labelsExpr = m.filter { it.key != nameLabel }.map { "${it.key}=\"${it.value}\"" }.joinToString(",")
        val n = m[nameLabel]
        val nameLabel = when (n) {
            null -> ""
            "" -> "\"\""
            else -> n
        }
        return "$nameLabel{$labelsExpr}"
    }

    override fun equals(other: Any?): Boolean {
        if (other === null) {
            return false
        }
        if (other === literal) {
            return false
        }
        val o = other as? Metric ?: return false
        if (o.m.size != this.m.size) {
            return false
        }
        return this.m.all { kv ->
            o.m[kv.key] == kv.value
        }
    }

    override fun hashCode(): Int {
        return m.hashCode()
    }

    companion object {
        val SeparatorByte: Byte = 0xFF.toByte()
        val nameLabel = "__name__"
        val instanceLabel = "instance"

        val empty = Metric(sortedMapOf())

        val literal = object : Metric(sortedMapOf()) {
            override fun equals(other: Any?): Boolean {
                return other === this
            }

            override fun toString(): String {
                return "{Literal}"
            }
        }

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

        fun of(name: String, vararg labels: Pair<String, String>): Metric {
            return Metric(sortedMapOf(nameLabel to name, *labels))
        }

        fun labelsCanonicalizedID(m: SortedMap<String, String>): String {
            val sb = StringBuilder()
            m.forEach { k, v ->
                sb.append(k)
                sb.append('=')
                sb.append(v)
                sb.append('\uffff')
            }
            return sb.toString()
        }


        fun labelFilteredFingerprintFNV(on: Boolean, m: SortedMap<String, String>, onLabels: Collection<String>): Signature {
            var res = FNV64.create()
            m.forEach { k, v ->
                if ((!on) xor onLabels.contains(k)) {
                    var sum = FNV64.create()
                    sum = FNV64.update(sum, k.toByteArray())
                    sum = FNV64.update(sum, SeparatorByte)
                    sum = FNV64.update(sum, v.toByteArray())
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
