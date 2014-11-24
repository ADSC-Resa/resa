package resa.scheduler.plan;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by ding on 14-7-23.
 */
public class SimpleCalculator extends PackCalculator {

    private double gain = -1.0;
    private int[] pack = null;
    private KuhnMunkres kmAlg;

    @Override
    protected Pack calcPack() {
        int maxPackSize = Stream.of(srcPacks).map(Map.Entry::getKey).mapToInt(pack -> pack.length).max().getAsInt();
        kmAlg = new KuhnMunkres(Math.max(packSize, maxPackSize));
        split(0, new int[0], packSize);
        kmAlg = null;
        if (pack == null) {
            throw new IllegalStateException("No pack is found");
        }
        Pack ret = new Pack(pack, gain);
        gain = -1.0;
        pack = null;
        return ret;
    }

    private void split(int start, int[] spilts, int part) {
        if (start + part > workloads.length) {
            return;
        }
        if (part == 1) {
            if (start == workloads.length || spilts.length + 1 != packSize) {
                throw new IllegalStateException("start == workloads.length");
            } else if (totalWorkload(start, workloads.length) <= loadUpperLimit) {
                int[] newPack = new int[spilts.length + 1];
                System.arraycopy(spilts, 0, newPack, 0, spilts.length);
                newPack[newPack.length - 1] = workloads.length - start;
                double g = totalGain(convertPack(newPack));
                if (g > gain) {
                    gain = g;
                    pack = newPack;
                }
            }
            return;
        }
        int[] newSplits = new int[spilts.length + 1];
        System.arraycopy(spilts, 0, newSplits, 0, spilts.length);
        for (int i = start + 1; i < workloads.length; i++) {
            if (totalWorkload(start, i) > loadUpperLimit) {
                break;
            }
            newSplits[newSplits.length - 1] = i - start;
            split(i, newSplits, part - 1);
        }
    }

    private double totalGain(Range[] pack) {
        return Stream.of(srcPacks).mapToDouble(e -> packGain(pack, e.getKey()) * e.getValue()).sum();
    }

    private double packGain(Range[] pack1, Range[] pack2) {
        double[][] weights = new double[pack1.length][pack2.length];
        for (int i = 0; i < pack1.length; i++) {
            for (int j = 0; j < pack2.length; j++) {
                weights[i][j] = overlap(pack1[i], pack2[j]);
            }
        }
        double[] maxWeight = new double[1];
        kmAlg.getMaxBipartie(weights, maxWeight);
        // int[][] pairs = kmAlg.getMaxBipartie(weights, maxWeight);
        // return Stream.of(pairs).mapToDouble(r -> overlap(newPacks[r[0]], currPacks[pStart + r[1]])).sum();
        return maxWeight[0];
    }

    private double overlap(Range r1, Range r2) {
        if (r1.end < r2.start || r1.start > r2.end) {
            return 0;
        } else if (r1.start <= r2.start && r1.end >= r2.start) {
            return IntStream.rangeClosed(r2.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        } else if (r1.start >= r2.start && r1.start <= r2.end) {
            return IntStream.rangeClosed(r1.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        }
        return 0;
    }

    private double totalWorkload(int wStart, int wEnd) {
        double sum = 0;
        for (int i = wStart; i < wEnd; i++) {
            sum += normalizedWordloads[i];
        }
        return sum;
    }

}
