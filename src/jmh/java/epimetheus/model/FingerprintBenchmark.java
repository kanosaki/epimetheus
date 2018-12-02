package epimetheus.model;

import java.util.Collection;
import java.util.Collections;
import kotlin.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class FingerprintBenchmark {
    private Metric[] metrics;
    private int samplesCount = 1000;
    private Collection<String> filtering = Collections.singletonList("other");

    public FingerprintBenchmark() {
        metrics = initMetrics();
    }

    private Metric[] initMetrics() {
        Metric[] ret = new Metric[samplesCount];
        for (int i = 0; i < samplesCount; i++) {
            ret[i] = Metric.Companion.of("a", new Pair<>("num", String.format("%d", i)), new Pair<>("other", "hogehoge"));
        }
        return ret;
    }

    @Benchmark
    public void fnvAll() {
        for (int i = 0; i < samplesCount; i++) {
            Metric.Companion.labelsFingerprintFNV(metrics[i].toSortedMap());
        }
    }

    @Benchmark
    public void xxHashAll() {
        for (int i = 0; i < samplesCount; i++) {
            Metric.Companion.labelsFingerprintXXHash(metrics[i].toSortedMap());
        }
    }

    @Benchmark
    public void fnvFilteredOn() {
        for (int i = 0; i < samplesCount; i++) {
            Metric.Companion.labelFilteredFingerprintFNV(true, metrics[i].getLbls(), filtering, true);
        }
    }

    @Benchmark
    public void fnvFilteredWithout() {
        for (int i = 0; i < samplesCount; i++) {
            Metric.Companion.labelFilteredFingerprintFNV(false, metrics[i].getLbls(), filtering, true);
        }
    }

}
