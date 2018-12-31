package epimetheus.pkg.textparse

import org.antlr.v4.runtime.CharStreams
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.test.assertEquals

class ParseTest {

    @Test
    fun parseLine() {
        //val ast = Parser.parseToEnd("foo:bar:baz{piyo=\"hoge\",abc=\"xyz\"} 123.43")
        val ast = ExporterParser.parse(CharStreams.fromString("foo:bar{hoge=\"fuga\"} 123.01E3"))
        assertEquals(listOf(ScrapedSample.create("foo:bar", 123.01E3, "hoge" to "fuga")), ast)
    }

    @Test
    fun parseFloatingLiterals() {
        val cases = listOf(
                "1" to 1.0,
                "Inf" to Double.POSITIVE_INFINITY,
                "+Inf" to Double.POSITIVE_INFINITY,
                "-Inf" to Double.NEGATIVE_INFINITY,
                "NaN" to Double.NaN,
                ".5" to 0.5,
                "5." to 5.0,
                "123.4567" to 123.4567,
                "+5.5e-3" to 5.5e-3,
                "-50.5e-3" to -50.5e-3
        )
        cases.forEach {
            val ast = ExporterParser.parse(CharStreams.fromString(it.first))
            assertEquals(listOf(ScrapedSample.create("", it.second)), ast)
        }
    }

    /**
     * Parse sample exporter data without exception
     */
    @Test
    fun parseSamplePage() {
        File("src/test/resources/exporter_testdata.txt").inputStream().use { input ->
            ExporterParser.parse(CharStreams.fromStream(input))
        }
    }
}