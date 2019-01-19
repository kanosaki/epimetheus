package epimetheus.pkg.serializer

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import java.time.Duration

class DurationSerializer : JsonSerializer<Duration>() {
    override fun serialize(value: Duration?, gen: JsonGenerator?, serializers: SerializerProvider?) {
        if (value == null) {
            gen?.writeNull()
        } else {
            gen?.writeNumber(value.toMillis())
        }
    }
}