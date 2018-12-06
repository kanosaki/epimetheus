package epimetheus.pkg.promql;

import org.antlr.v4.runtime.CharStreams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class ParseBench {
    String[] queries = new String[] {
        // queries from node_exporter (0.15) dashboard
        "(((count(count(node_cpu{instance=~\"undefined:undefined\"}) by (cpu))) - avg(sum by (mode)(irate(node_cpu{mode='idle',instance=~\"undefined:undefined\"}[5m])))) * 100) / count(count(node_cpu{instance=~\"undefined:undefined\"}) by (cpu))\n",
        "((node_memory_SwapTotal{instance=~\"undefined:undefined\"} - node_memory_SwapFree{instance=~\"undefined:undefined\"}) / (node_memory_SwapTotal{instance=~\"undefined:undefined\"} )) * 100\n",
        "(node_memory_SwapTotal{instance=~\"undefined:undefined\"} - node_memory_SwapFree{instance=~\"undefined:undefined\"})\n",
        "100 - ((node_filesystem_avail{instance=~\"undefined:undefined\",device!~'rootfs'} * 100) / node_filesystem_size{instance=~\"undefined:undefined\",device!~'rootfs'})\n",
        "100 - ((node_filesystem_avail{instance=~\"undefined:undefined\",mountpoint=\"/\",fstype!=\"rootfs\"} * 100) / node_filesystem_size{instance=~\"undefined:undefined\",mountpoint=\"/\",fstype!=\"rootfs\"})\n",
        "100 - ((node_memory_MemAvailable{instance=~\"undefined:undefined\"} * 100) / node_memory_MemTotal{instance=~\"undefined:undefined\"})\n",
        "avg(node_load1{instance=~\"undefined:undefined\"}) /  count(count(node_cpu{instance=~\"undefined:undefined\"}) by (cpu)) * 100\n",
        "count(count(node_cpu{instance=~\"undefined:undefined\"}) by (cpu))\n",
        "irate(node_context_switches{instance=~\"undefined:undefined\"}[5m])\n",
        "irate(node_disk_bytes_read{instance=~\"undefined:undefined\",device=~\"[a-z]*[a-z]\"}[5m])\n",
        "irate(node_vmstat_pgfault{instance=~\"undefined:undefined\"}[5m])  - irate(node_vmstat_pgmajfault{instance=~\"undefined:undefined\"}[5m])\n",
        "sum (rate(node_cpu{mode!='idle',mode!='user',mode!='system',mode!='iowait',mode!='irq',mode!='softirq',instance=~\"undefined:undefined\"}[5m])) * 100\n",
        "sum by (instance)(rate(node_cpu{mode=\"system\",instance=~\"undefined:undefined\"}[5m])) * 100\n",
        "sum by (mode)(irate(node_cpu{mode=\"system\",instance=~\"undefined:undefined\"}[5m])) * 100\n",
    };

    @Benchmark
    public void q1() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[0]), false);
    }

    @Benchmark
    public void q2() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[1]), false);
    }

    @Benchmark
    public void q3() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[2]), false);
    }

    @Benchmark
    public void q4() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[3]), false);
    }

    @Benchmark
    public void q5() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[4]), false);
    }

    @Benchmark
    public void q6() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[5]), false);
    }

    @Benchmark
    public void q7() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[6]), false);
    }

    @Benchmark
    public void q8() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[7]), false);
    }

    @Benchmark
    public void q9() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[8]), false);
    }

    @Benchmark
    public void q10() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[9]), false);
    }

    @Benchmark
    public void q11() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[10]), false);
    }

    @Benchmark
    public void q12() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[11]), false);
    }

    @Benchmark
    public void q13() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[12]), false);
    }

    @Benchmark
    public void q14() {
        PromQL.INSTANCE.parse(CharStreams.fromString(queries[13]), false);
    }
}
