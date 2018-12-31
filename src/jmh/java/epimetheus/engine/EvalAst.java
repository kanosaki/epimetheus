package epimetheus.engine;

import epimetheus.model.TimeFrames;
import epimetheus.pkg.promql.Expression;
import epimetheus.pkg.promql.PromQL;
import epimetheus.pkg.textparse.ScrapedSample;
import epimetheus.storage.Gateway;
import epimetheus.storage.MockGateway;
import java.util.Arrays;
import java.util.List;
import kotlin.Pair;
import org.antlr.v4.runtime.CharStreams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Thread)
public class EvalAst {
    private Interpreter interp;
    private Engine engine;
    private Gateway gateway;
    private Expression[] exprs;

    public EvalAst() {
        gateway = new MockGateway();
        interp = new Interpreter(gateway, 1000L);
        engine = new Engine(gateway, 1000L);

        String[] codes = new String[] {
            "1",
            "1 + 2",
            "a + b + c",
            "rate(a[1m])",
            "max by (instance) (a)",
            "max by (instance) (rate(a[1m]))",
        };
        exprs = new Expression[codes.length];
        for (int i = 0; i < codes.length; i++) {
            exprs[i] = PromQL.INSTANCE.parse(CharStreams.fromString(codes[i]), false);
        }
        for (int ts = 0; ts < 100 * 100; ts += 100) {
            for (int instance = 0; instance < 100; instance++) {
                List<ScrapedSample> samples = Arrays.asList(
                    ScrapedSample.Companion.create("a", (double)ts, new Pair<>("foo", "bar"), new Pair<>("instance", Integer.toString(instance))),
                    ScrapedSample.Companion.create("b", (double)ts, new Pair<>("foo", "bar"), new Pair<>("instance", Integer.toString(instance))),
                    ScrapedSample.Companion.create("c", (double)ts, new Pair<>("foo", "bar"), new Pair<>("instance", Integer.toString(instance))),
                    ScrapedSample.Companion.create("d", (double)ts, new Pair<>("foo", "bar"), new Pair<>("instance", Integer.toString(instance))),
                    ScrapedSample.Companion.create("e", (double)ts, new Pair<>("foo", "bar"), new Pair<>("instance", Integer.toString(instance)))
                );
                gateway.pushScraped(ts, samples, false);
            }
        }
        gateway.pushScraped(0, Arrays.asList(), true);
    }

    @Benchmark
    public void simplePlusInterp() {
        interp.evalAst(exprs[1], TimeFrames.Companion.instant(1), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void simplePlusEngine() {
        engine.evalAst(exprs[1], TimeFrames.Companion.instant(1), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void selectorPlusInterp() {
        interp.evalAst(exprs[2], TimeFrames.Companion.instant(100), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void selectorPlusEngine() {
        engine.evalAst(exprs[2], TimeFrames.Companion.instant(100), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void rateInterp() {
        interp.evalAst(exprs[3], TimeFrames.Companion.instant(100), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void rateEngine() {
        engine.evalAst(exprs[3], TimeFrames.Companion.instant(100), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void maxByInterp() {
        interp.evalAst(exprs[4], TimeFrames.Companion.instant(100), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void maxByEngine() {
        engine.evalAst(exprs[4], TimeFrames.Companion.instant(100), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void maxRateInterp() {
        interp.evalAst(exprs[5], TimeFrames.Companion.instant(100), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void maxRateEngine() {
        engine.evalAst(exprs[5], TimeFrames.Companion.instant(100), Tracer.Companion.getEmpty());
    }
}
