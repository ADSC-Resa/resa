package resa.evaluation.scheduler.plan;

import org.junit.Before;
import org.junit.Test;
import resa.migrate.plan.ExecutionAnalyzer;
import resa.util.RedisQueueIterable;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public class ExecutionAnalyzerTest {

    private double[] dataSizes;
    private double[] workload;
    private TreeMap<String, Double> migrationMetrics;
    private float ratio = 1.8f;
    private int[] allStates;

    @Before
    public void init() throws Exception {
        migrationMetrics = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/metrics.txt")).stream()
                .map(s -> s.split(":")).collect(Collectors.toMap(strs -> strs[0] + "-" + strs[1],
                        strs -> Double.parseDouble(strs[2]), (v1, v2) -> {
                            throw new IllegalStateException();
                        }, TreeMap::new));
        allStates = migrationMetrics.keySet().stream().flatMapToInt(s -> {
            String[] tmp = s.split("-");
            return IntStream.of(Integer.parseInt(tmp[0]), Integer.parseInt(tmp[1]));
        }).boxed().collect(Collectors.toSet()).stream().mapToInt(i -> i).toArray();
    }

    @Test
    public void testCalcStat() throws Exception {
        String compName = "detector";
        SortedMap<String, ExecutionAnalyzer.ExecutionStat> ret;
        try (RedisQueueIterable data = new RedisQueueIterable("192.168.0.30", 6379, "fpt-16-1409727090-metrics", 980000)) {
            ExecutionAnalyzer analyzer = new ExecutionAnalyzer(data);
            analyzer.calcStat();
            ret = analyzer.getStat().subMap(compName, compName + "~");
        }
        dataSizes = ret.values().stream().mapToDouble(s -> s.getDataSize()).toArray();
        workload = ret.values().stream().mapToDouble(s -> s.getCost()).toArray();
        DoubleStream.of(dataSizes).map(size -> size / (1024 * 1024)).forEach(System.out::println);
        System.out.println("Avg data size " + (DoubleStream.of(dataSizes).sum() / 1024 / 1024 / dataSizes.length) + "MB");
        DoubleStream.of(workload).forEach(System.out::println);
    }

}