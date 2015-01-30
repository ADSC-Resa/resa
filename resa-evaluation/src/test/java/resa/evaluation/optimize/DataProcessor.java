package resa.evaluation.optimize;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-10-13.
 */
public class DataProcessor {

    @Test
    public void calcAvg() throws IOException {
        List<String> line = Files.lines(Paths.get("/tmp/data.txt")).filter(str -> !str.isEmpty())
                .collect(Collectors.toList());
        int batchSize = 4;
        int loop = line.size() / batchSize;
        long count = 503;
        double sum = 25000 * count;
        for (int i = 0; i < loop; i++) {
            for (int j = i * batchSize; j < (i + 1) * batchSize; j++) {
                String[] tmp = line.get(j).split(",");
                sum += Double.parseDouble(tmp[1]);
                count += Long.parseLong(tmp[0]);
            }
        }
        System.out.printf("%.2f\n", (sum / count));
    }

}
