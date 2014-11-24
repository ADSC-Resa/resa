package resa.evaluation.scheduler.plan;

import org.junit.Test;
import resa.util.Counter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Created by ding on 14-8-13.
 */
public class DataProcessor {

    @Test
    public void process() throws IOException {
        long[] result = new long[11];
        Counter counter = new Counter(0);
        Files.lines(Paths.get("/Users/ding/Desktop/fp-opt-data.txt")).filter(s -> !s.isEmpty()).forEach(line -> {
            long[] stat = Stream.of(line.replace('|', ',').split(",")).mapToLong(Long::parseLong).toArray();
            for (int i = 0; i < stat.length; i++) {
                result[i] += stat[i];
            }
            counter.incAndGet();
        });
        int cnt = (int) (counter.get() / 2);
        System.out.println("Count is " + cnt);
        System.out.println(Arrays.toString(LongStream.of(result).map(l -> l / cnt).toArray()));
        System.out.println("Total is " + LongStream.of(result).map(l -> l / cnt).sum());
    }

    @Test
    public void getAvg() throws IOException {
        double[] result = new double[2];
        Files.lines(Paths.get("/Users/ding/Desktop/fp-opt-move-70.txt")).filter(s -> !s.isEmpty()).forEach(line -> {
            String[] tmp = line.split(",");
            result[0] += Double.valueOf(tmp[0]);
            result[1] += Double.valueOf(tmp[1]);
        });
        result[0] += 0;
        result[1] += 0 * 25000;
        System.out.println("total " + (long) result[0]);
        System.out.println("Avg is " + (result[1] / result[0]));
    }

    @Test
    public void calcSum() {
        int sum = Stream.of("213,18,16,20,23,27,27,26,29,39,44".split(",")).mapToInt(Integer::parseInt).sum();
        System.out.println(sum);
    }

}
