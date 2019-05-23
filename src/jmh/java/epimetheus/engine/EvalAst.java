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
    private Engine engine;
    private Gateway gateway;
    private Expression[] exprs;

    public EvalAst() {
        gateway = new MockGateway();
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
    public void simplePlusEngine() {
        ExecContext ec = new ExecContext(TimeFrames.Companion.instant(1), NopTracer.INSTANCE);
        engine.evalAst(exprs[1], ec);
    }


    @Benchmark
    public void selectorPlusEngine() {
        ExecContext ec = new ExecContext(TimeFrames.Companion.instant(1), NopTracer.INSTANCE);
        engine.evalAst(exprs[2], ec);
    }


    @Benchmark
    public void rateEngine() {
        ExecContext ec = new ExecContext(TimeFrames.Companion.instant(1), NopTracer.INSTANCE);
        engine.evalAst(exprs[3], ec);
    }

    @Benchmark
    public void maxByEngine() {
        ExecContext ec = new ExecContext(TimeFrames.Companion.instant(1), NopTracer.INSTANCE);
        engine.evalAst(exprs[4], ec);
    }

    @Benchmark
    public void maxRateEngine() {
        ExecContext ec = new ExecContext(TimeFrames.Companion.instant(1), NopTracer.INSTANCE);
        engine.evalAst(exprs[5], ec);
    }
}
