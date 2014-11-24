package resa.scheduler.plan;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-6-28.
 */
public class SinglePackCalculator extends PackCalculator {

    private Map<String, Pack> cache;
    private KuhnMunkres kmAlg;
    private Range[] currPack;

    protected Pack calcPack() {
        currPack = srcPacks[0].getKey();
        kmAlg = new KuhnMunkres(Math.max(packSize, currPack.length));
        Pack p;
        if (normalizedWordloads.length == packSize) {
            int[] ret = new int[normalizedWordloads.length];
            Arrays.fill(ret, 1);
            Range[] packs = IntStream.range(0, packSize).mapToObj(i -> new Range(i, i)).toArray(Range[]::new);
            p = new Pack(ret, gain(packs, 0, currPack.length));
        } else {
            //init buffer
            cache = new HashMap<>();
            p = calc0(0, normalizedWordloads.length, packSize, 0, currPack.length);
            // release memory
            cache = null;
        }
        kmAlg = null;
        return p;
    }

    private Pack calc0(int wStart, int wEnd, int numPartition, int pStart, int pEnd) {
        if (wEnd - wStart < numPartition || numPartition == 0) {
            throw new IllegalStateException("start=" + wStart + ", end=" + wEnd + ", numPartition=" + numPartition);
        }
        String key = wStart + "-" + wEnd + "-" + numPartition + "-" + pStart + "-" + pEnd;
        return cache.computeIfAbsent(key, k -> calc1(wStart, wEnd, numPartition, pStart, pEnd));
    }

    private Pack calc1(int wStart, int wEnd, int numPartition, int pStart, int pEnd) {
        if (numPartition == 1) {
            double sum = totalWorkload(wStart, wEnd);
            return sum > loadUpperLimit ? INFEASIBLE :
                    new Pack(new int[]{wEnd - wStart}, gain(new Range[]{new Range(wStart, wEnd - 1)}, pStart, pEnd));
        } else if (wEnd - wStart == numPartition) {
            int[] ret = new int[numPartition];
            Arrays.fill(ret, 1);
            Range[] newRanges = IntStream.range(wStart, wEnd).mapToObj(i -> new Range(i, i)).toArray(Range[]::new);
            return new Pack(ret, gain(newRanges, pStart, pEnd));
        }
        int split = wStart + 1;
        Pack rightPack = INFEASIBLE;
        double gain = Double.MIN_VALUE;
        for (int i = split; i < wEnd; i++) {
            if (totalWorkload(wStart, i) > loadUpperLimit || wEnd - i < numPartition - 1) {
                break;
            }
            int startPack = findPack(i);
            int stopPack = currPack[startPack].start < i ? startPack + 1 : startPack;
            startPack = Math.max(startPack, pStart);
            stopPack = Math.min(stopPack, pEnd);
            for (int k = startPack; k <= stopPack; k++) {
                Pack left = calc0(wStart, i, 1, pStart, k);
                Pack right = calc0(i, wEnd, numPartition - 1, k, pEnd);
                if (left != INFEASIBLE && right != INFEASIBLE) {
                    double newGain = left.gain + right.gain;
                    if (Double.compare(newGain, gain) > 0) {
                        gain = newGain;
                        split = i;
                        rightPack = right;
                    }
                }
            }
        }
        if (rightPack == INFEASIBLE) {
            return INFEASIBLE;
        }
        int[] ret = new int[numPartition];
        ret[0] = split - wStart;
        System.arraycopy(rightPack.packing, 0, ret, 1, numPartition - 1);
        return new Pack(ret, gain);
    }

    private int findPack(int v) {
        for (int i = 0; i < currPack.length; i++) {
            if (currPack[i].end >= v) {
                return i;
            }
        }
        throw new IllegalStateException("value is " + v);
    }

    private double totalWorkload(int wStart, int wEnd) {
        double sum = 0;
        for (int i = wStart; i < wEnd; i++) {
            sum += normalizedWordloads[i];
        }
        return sum;
    }

    private double gain(Range[] newPacks, int pStart, int pEnd) {
        if (newPacks.length == 0 || pStart == pEnd) {
            return 0;
        }
        double[][] weights = new double[newPacks.length][pEnd - pStart];
        for (int i = 0; i < weights.length; i++) {
            for (int j = 0; j < weights[i].length; j++) {
                weights[i][j] = overlap(newPacks[i], currPack[pStart + j]);
            }
        }
        double[] maxWeight = new double[1];
        kmAlg.getMaxBipartie(weights, maxWeight);
        // int[][] pairs = kmAlg.getMaxBipartie(weights, maxWeight);
        // return Stream.of(pairs).mapToDouble(r -> overlap(newPacks[r[0]], currPack[pStart + r[1]])).sum();
        return maxWeight[0];
    }

    private double overlap(Range r1, Range r2) {
        if (r1.start <= r2.start && r1.end >= r2.start) {
            return IntStream.rangeClosed(r2.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        } else if (r1.start >= r2.start && r1.start <= r2.end) {
            return IntStream.rangeClosed(r1.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        }
        return 0;
    }

}
