package epimetheus.pkg.serializer

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import java.time.Duration

class DurationDeserializer : JsonDeserializer<Duration?>() {
    override fun deserialize(p: JsonParser?, ctxt: DeserializationContext?): Duration? {
        if (p == null) return null
        val txt = p.text
        when {
            txt.endsWith("s") -> {
                val v = txt.removeSuffix("s")
                return Duration.ofMillis((v.toDouble() * 1000).toLong())
            }
            txt.endsWith("m") -> {
                val v = txt.removeSuffix("m")
                return Duration.ofMillis((v.toDouble() * 1000 * 60).toLong())

            }
            txt.endsWith("h") -> {
                val v = txt.removeSuffix("h")
                return Duration.ofMillis((v.toDouble() * 1000 * 60 * 60).toLong())
            }
        }
        return Duration.ofMillis(txt.toDouble().toLong())
    }
}

