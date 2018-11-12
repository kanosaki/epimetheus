package epimetheus.engine

import epimetheus.model.*
import epimetheus.pkg.promql.Expression
import epimetheus.pkg.promql.PromQL
import epimetheus.pkg.promql.PromQLException
import epimetheus.pkg.textparse.InvalidSpecLine
import epimetheus.pkg.textparse.ScrapedSample
import epimetheus.storage.Gateway
import epimetheus.storage.MockGateway
import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.CharStreams
import org.apache.commons.io.IOUtils
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.fail
import org.junit.jupiter.api.function.Executable
import java.io.File
import java.time.Duration


class SpecContext(var storage: Gateway, var interpreter: Interpreter) {
}

abstract class SpecCmd {
    abstract fun eval(ctx: SpecContext)
    var lineNo: Int? = null
    var lineStr: String? = null
}

/**
 * Runs Prometheus test cases(*.test files)
 */
class PromSpec(val lines: List<String>) : Executable {
    companion object {
        private val patEvalInstant = Regex("""\s*instant\s+(?:at\s+(\S+)\s*)?(.+)${'$'}""")
    }

    val commands by lazy { parse() }

    private fun parse(): List<SpecCmd> {
        val cmds = mutableListOf<SpecCmd>()
        var ctr = 0
        var lastLineError: Throwable? = null

        fun nextLine(): String? {
            while (ctr < lines.size) {
                val line = lines[ctr]
                ctr++
                if (!line.startsWith("#") && !line.isEmpty()) {
                    return line
                }
            }
            return null
        }

        /**
         * parse upcoming lines by given parseFn until it throws an error
         * and returns successfully parsed values
         */
        fun <T> forwardParse(parseFn: (CharStream) -> T): List<T> {
            val specs = mutableListOf<T>()
            try {
                var line: String? = nextLine()
                while (line != null) {
                    specs += parseFn(CharStreams.fromString(line))
                    line = nextLine()
                }
            } catch (ex: InvalidSpecLine) {
                lastLineError = ex
                ctr--
            } catch (ex: PromQLException) {
                lastLineError = ex
                ctr--
            }
            return specs
        }

        /**
         * load <duration>
         */
        fun parseLoad(line: String): SpecCmd {
            val specs = forwardParse(SpecSeriesParser::parseLine)
            val duration = PromQL.parseDuration(CharStreams.fromString(line))
            // cannot use literal spec in load cmd
            return LoadCmd(duration, specs.map { it as SpecSeriesDesc })
        }

        /**
         * eval{_ordered,_fail,} instant [at <duration>] <expr>
         */
        fun parseEval(line: String, ordered: Boolean, fail: Boolean): SpecCmd {
            assert(!(ordered and fail)) { "you cannot specify `ordered` and `fail` same time" }
            val specs = forwardParse(SpecSeriesParser::parseLine)
            val m = patEvalInstant.find(line) ?: throw RuntimeException("Invalid eval command: $line")
            var offset: Duration? = null
            val at = m.groups[1]
            if (at != null) {
                offset = PromQL.parseDuration(CharStreams.fromString(at.value))
            }
            val exprStr = m.groups[2] ?: throw RuntimeException("No expr found in eval cmd: $line")
            val expr = PromQL.parse(CharStreams.fromString(exprStr.value))
                    ?: throw RuntimeException("Invalid expression")
            return EvalCmd(expr, offset, ordered, fail, specs)
        }

        var line: String? = nextLine()
        try {
            while (line != null) {
                val tokens = line!!.split(' ', limit = 2)
                if (tokens.isEmpty()) {
                    throw RuntimeException("Invalid line $line")
                }
                val cmdCtr = ctr - 1
                val cmd = when (tokens[0].toLowerCase()) {
                    "load" -> parseLoad(tokens[1])
                    "clear" -> ClearCmd
                    "eval" -> parseEval(tokens[1], false, false)
                    "eval_ordered" -> parseEval(tokens[1], true, false)
                    "eval_fail" -> parseEval(tokens[1], false, true)
                    else -> throw RuntimeException("Invalid command: ${tokens[0]}")
                }
                cmd.apply {
                    lineNo = cmdCtr
                    lineStr = lines[cmdCtr]
                }
                cmds += cmd
                line = nextLine()
            }
        } catch (ex: Throwable) {
            println("""DEBUG
                | Command stack:
                |   ${cmds.map { "${it.lineStr} at ${it.lineNo} $it" }.joinToString("\n  ")}
                | Last Error: $lastLineError
                | """.trimMargin())
            throw ex
        }
        return cmds
    }

    data class LoadCmd(val gap: Duration, val specs: List<SpecSeriesDesc>) : SpecCmd() {
        override fun eval(ctx: SpecContext) {
            var currentTs: Long = 0
            for (spec in specs) {
                spec.expand(null).forEach {
                    // null -> blank value, skip and just increment timestamp
                    if (it != null) {
                        val instance = spec.m[Metric.instanceLabel] ?: ""
                        ctx.storage.pushScraped(instance, currentTs, listOf(ScrapedSample.create(spec.name, it, *spec.labels)))
                    }
                    currentTs += gap.toMillis()
                }
            }
        }
    }

    data class EvalCmd(val expr: Expression, val offset: Duration?, val ordered: Boolean, val fail: Boolean, val specs: List<Spec>) : SpecCmd() {
        override fun eval(ctx: SpecContext) {
            val tf = if (offset != null) {
                TimeFrames(offset.toMillis(), offset.toMillis() + 1, 1) // TODO: define instant selector
            } else {
                throw RuntimeException("offset required")
            }
            val tracer = FullLoggingTracer()
            val result = ctx.interpreter.evalAst(expr, tf, tracer)
            val series = specs.map {
                when (it) {
                    is SpecSeriesDesc -> {
                        Metric(it.m) to Mat.mapValue(it.expand(null))
                    }
                    is SpecLiteralDesc -> {
                        Metric.literal to Mat.mapValue(it.expand(null))
                    }
                    else -> throw RuntimeException("never here")
                }
            }
            val expected = Mat.instant(series.map { it.first }.toTypedArray(), offset.toMillis(), series.map { it.second[0] })
            try {
                TestUtils.assertValueEquals(expected, result, true, true)
            } catch (e: AssertionError) {
                // print executing status
                println("====== Eval TRACE")
                tracer.printTrace()
                println("====== Eval EXPECTED")
                println(expected)
                println("====== Eval ACTUAL")
                when (result) {
                    is GridMat -> println(result.toTable().printAll())
                    is Scalar -> println("SCALAR ${result.value}")
                    is VarMat -> println(result)
                    else -> println("UNKNOWN Value type: ${result.javaClass}: $result")
                }
                throw e
            }
        }
    }

    object ClearCmd : SpecCmd() {
        override fun eval(ctx: SpecContext) {
            // clear handled in externally (see execute() method)
            throw RuntimeException("never here")
        }
    }

    override fun execute() {
        var storage = MockGateway()
        var interpreter = Interpreter(storage)
        var ctx = SpecContext(storage, interpreter)
        commands.forEach {
            try {
                when (it) {
                    is ClearCmd -> {
                        storage = MockGateway()
                        interpreter = Interpreter(storage)
                        ctx = SpecContext(storage, interpreter)
                    }
                    else -> it.eval(ctx)
                }
            } catch (ex: Throwable) {
                fail("EXCEPTION during evaluating ${it.lineStr} (line: ${it.lineNo}) $it", ex)
            }
        }
    }
}

object PromSpecTests {
    /**
     * Spec files ported from prometheus repository
     */
    @TestFactory
    fun fromPrometheus(): Collection<DynamicTest> {
        val res = PromSpecTests::class.java.getResource("promspec/prometheus")
        val files = File(res.path).listFiles()
        return files.filter { it.name == "staleness.test" }.map {
            it.inputStream().use { input ->
                val lines = IOUtils.readLines(input, Charsets.UTF_8)
                        .map { it.trim() }
                DynamicTest.dynamicTest(it.name, PromSpec(lines))
            }
        }
    }
}