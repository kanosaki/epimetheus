package epimetheus.pkg.serializer

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals

class DurationDeserializerTest {
    data class Dummy(@JsonDeserialize(using = DurationDeserializer::class) val t: Duration)

    @Test
    fun testDurationDeserialize() {
        val mapper = jacksonObjectMapper()
        val v = mapper.readValue<Dummy>("{\"t\": \"3s\"}")
        assertEquals(3000, v.t.toMillis())
    }
}