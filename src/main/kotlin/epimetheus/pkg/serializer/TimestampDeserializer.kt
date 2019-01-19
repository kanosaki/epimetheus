package epimetheus.pkg.serializer

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import java.time.Instant
import java.time.LocalDateTime
import java.util.*

class TimestampDeserializer : JsonDeserializer<LocalDateTime?>() {
    override fun deserialize(p: JsonParser?, ctxt: DeserializationContext?): LocalDateTime? {
        if (p == null) {
            return null
        }
        return LocalDateTime
                .ofInstant(
                        Instant.ofEpochMilli(p.numberValue.toLong()),
                        TimeZone.getDefault().toZoneId()
                )
    }
}