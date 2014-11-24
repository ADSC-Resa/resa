package resa.scheduler.plan;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.DoubleStream;

/**
 * Created by ding on 14-6-3.
 */
public class PackingAlg {

    private static class Pack {
        int[] packing;
        double sumOfDiffSquare;

        Pack(int[] packing, double sumOfDiffSquare) {
            this.packing = packing;
            this.sumOfDiffSquare = sumOfDiffSquare;
        }
    }

    public static int[] calc(double[] workloads, int numPartition) {
        if (workloads.length < numPartition) {
            throw new IllegalArgumentException("numPartition cann't larger than workloads size");
        } else if (workloads.length == numPartition) {
            int[] ret = new int[workloads.length];
            Arrays.fill(ret, 1);
            return ret;
        }
        double avg = DoubleStream.of(workloads).sum() / numPartition;
        Map<String, Pack> cache = new HashMap<>();
        calc0(workloads, 0, workloads.length, avg, numPartition, cache);
        int[] ret = cache.get("0-" + workloads.length + "-" + numPartition).packing;
        return Objects.requireNonNull(ret, "Cannot arrived here");
    }

    public static double calc0(double[] workloads, int start, int end, double avg, int numPartition,
                               Map<String, Pack> cache) {
        if (end - start < numPartition || numPartition == 0) {
            throw new IllegalStateException("start=" + start + ", end=" + end + ", numPartition=" + numPartition);
        }
        String key = start + "-" + end + "-" + numPartition;
        Pack pack = cache.get(key);
        if (pack != null) {
            return pack.sumOfDiffSquare;
        } else if (numPartition == 1) {
            double sum = calcDiffSquare(workloads, start, end, avg);
            cache.put(key, new Pack(new int[]{end - start}, sum));
            return sum;
        } else if (end - start == numPartition) {
            int[] ret = new int[numPartition];
            double sum = 0;
            for (int i = 0; i < numPartition; i++) {
                ret[i] = 1;
                double diff = workloads[start + i] - avg;
                sum = sum + diff * diff;
            }
            cache.put(key, new Pack(ret, sum));
            return sum;
        }
        int split = start;
        double sum = Double.MAX_VALUE;
        for (int i = start; i < end - 1 && end - i >= numPartition; i++) {
            double newSum = calcDiffSquare(workloads, start, i + 1, avg)
                    + calc0(workloads, i + 1, end, avg, numPartition - 1, cache);
            if (newSum < sum) {
                sum = newSum;
                split = i;
            }
        }
        int[] ret = new int[numPartition];
        ret[0] = split - start + 1;
        System.arraycopy(cache.get((split + 1) + "-" + end + "-" + (numPartition - 1)).packing, 0, ret, 1,
                numPartition - 1);
        cache.put(key, new Pack(ret, sum));
        return sum;
    }

    private static double calcDiffSquare(double[] workloads, int start, int end, double avg) {
        double diff = avg;
        for (int i = start; i < end; i++) {
            diff = diff - workloads[i];
        }
        return diff * diff;
    }

}
