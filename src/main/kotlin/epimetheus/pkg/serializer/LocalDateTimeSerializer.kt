package epimetheus.pkg.serializer

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import java.time.LocalDateTime
import java.time.ZoneId

class LocalDateTimeSerializer : JsonSerializer<LocalDateTime>() {
    companion object {
        val zone = ZoneId.systemDefault()
    }

    override fun serialize(value: LocalDateTime?, gen: JsonGenerator?, serializers: SerializerProvider?) {
        if (value == null) {
            gen?.writeNull()
        } else {
            gen?.writeNumber(value.atZone(zone).toInstant().toEpochMilli())
        }
    }
}