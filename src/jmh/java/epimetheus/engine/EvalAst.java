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
    private Gateway gateway;
    private Expression[] exprs;

    public EvalAst() {
        gateway = new MockGateway();
        interp = new Interpreter(gateway, 1000L);

        String[] codes = new String[] {
            "1",
            "1 + 2",
            "a + b + c",
            "rate(a[1m])"
        };
        exprs = new Expression[codes.length];
        for (int i = 0; i < codes.length; i++) {
            exprs[i] = PromQL.INSTANCE.parse(CharStreams.fromString(codes[i]), false);
        }
        for (int ts = 0; ts < 100 * 100; ts += 100) {
            for (int instance = 0; instance < 100; instance++) {
                List<ScrapedSample> samples = Arrays.asList(
                    ScrapedSample.Companion.create("a", (double)ts, new Pair<>("foo", "bar")),
                    ScrapedSample.Companion.create("b", (double)ts, new Pair<>("foo", "bar")),
                    ScrapedSample.Companion.create("c", (double)ts, new Pair<>("foo", "bar")),
                    ScrapedSample.Companion.create("d", (double)ts, new Pair<>("foo", "bar")),
                    ScrapedSample.Companion.create("e", (double)ts, new Pair<>("foo", "bar"))
                );
                gateway.pushScraped(ts, samples, false);
            }
        }
        gateway.pushScraped(0, Arrays.asList(), true);
    }

    @Benchmark
    public void singleLiteral() {
        interp.evalAst(exprs[0], TimeFrames.Companion.instant(1), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void simplePlus() {
        interp.evalAst(exprs[1], TimeFrames.Companion.instant(1), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void selectorPlus() {
        interp.evalAst(exprs[2], TimeFrames.Companion.instant(100), Tracer.Companion.getEmpty());
    }

    @Benchmark
    public void rate() {
        interp.evalAst(exprs[3], TimeFrames.Companion.instant(100), Tracer.Companion.getEmpty());
    }
}
