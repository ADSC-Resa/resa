package resa.evaluation.scheduler.plan;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-9-3.
 */
public class MetricsGenerater {

    private int[] workloads;
    private int[] states = new int[]{8, 10, 12, 14, 16};

    @Before
    public void loadWorkloads() throws IOException {
        workloads = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/metrics-load.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToInt(Integer::parseInt).toArray();
        System.out.println("min: " + IntStream.of(workloads).min().getAsInt());
        System.out.println("max: " + IntStream.of(workloads).max().getAsInt());
    }

    @Test
    public void generateMetrics() {
        Map<String, Integer> migrations = new TreeMap<>();
        for (int i = 1; i < workloads.length; i++) {
            int prev = findState(workloads[i - 1]);
            int curr = findState(workloads[i]);
            if (prev != curr) {
                System.out.println(states[curr]);
                migrations.compute(states[prev] + ":" + states[curr], (k, v) -> v == null ? 1 : v + 1);
            }
        }
        int totalCount = migrations.values().stream().mapToInt(i -> i).sum();
        migrations.forEach((k, v) -> System.out.println(k + ":" + ((double) v / totalCount)));
    }

    private int findState(int load) {
        if (load < 50000) {
            return 0;
        } else if (load < 60000) {
            return 1;
        } else if (load < 70000) {
            return 2;
        } else if (load < 80000) {
            return 3;
        }
        return 4;
    }

}
