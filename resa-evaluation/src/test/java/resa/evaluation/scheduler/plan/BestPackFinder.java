package resa.evaluation.scheduler.plan;

import org.junit.Before;
import org.junit.Test;
import resa.migrate.plan.KuhnMunkres;
import resa.migrate.plan.PackCalculator;

import java.util.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static resa.migrate.plan.PackCalculator.convertPack;

/**
 * Created by ding on 14-9-4.
 */
public class BestPackFinder {

    private double workload[];
    private double[] dataSizes;
    private float ratio = 1.20f;
    private double totalDataSize;

    @Before
    public void init() {
        workload = new double[100];
        Arrays.fill(workload, 100);
        dataSizes = new double[100];
        Arrays.fill(dataSizes, 100);
        totalDataSize = DoubleStream.of(dataSizes).sum();
    }

    @Test
    public void generateAllPacks() {
        int[] allStates = new int[]{2, 3, 4};
        Map<Integer, int[][]> statePacks = new HashMap<>();
        for (int state : allStates) {
            double loadUpperLimit = DoubleStream.of(workload).sum() / state * ratio;
            List<int[]> packs = new ArrayList<>();
            genPacks(0, new int[0], state, state, loadUpperLimit, workload, packs);
            statePacks.put(state, packs.toArray(new int[0][]));
        }
        float[] values = new float[statePacks.get(allStates[allStates.length - 1]).length];
        String[] paths = new String[statePacks.get(allStates[allStates.length - 1]).length];
        Arrays.fill(paths, "");
        for (int i = allStates.length - 2; i >= 0; i--) {
            int[][] currPacks = statePacks.get(allStates[i]);
            float[] newValues = new float[currPacks.length];
            for (int j = 0; j < currPacks.length; j++) {
                int[][] nextPacks = statePacks.get(allStates[i + 1]);
                PackCost selected = selectTargetPack(convertPack(currPacks[j]), nextPacks,
                        values, 0.9f);

            }
        }
    }

    private PackCost selectTargetPack(PackCalculator.Range[] srcPack, int[][] allTargetPacks, float[] costVector,
                                      float gam) {
        return IntStream.range(0, allTargetPacks.length).parallel().mapToObj(i ->
                new PackCost(gam * costVector[i] + (totalDataSize - packGain(srcPack, convertPack(allTargetPacks[i]),
                        new KuhnMunkres(dataSizes.length))), i))
                .min(Comparator.<PackCost>naturalOrder()).get();
    }

    private static class PackCost implements Comparable<PackCost> {
        double cost;
        int pos;

        PackCost(double cost, int pos) {
            this.cost = cost;
            this.pos = pos;
        }

        @Override
        public int compareTo(PackCost o) {
            return Double.compare(cost, o.cost);
        }
    }

    private void genPacks(int start, int[] splits, int part, int totalNumPartitions,
                          double loadUpperLimit, double[] normalizedWordloads, List<int[]> packs) {
        if (start + part > workload.length) {
            return;
        }
        if (part == 1) {
            if (start == workload.length || splits.length + 1 != totalNumPartitions) {
                throw new IllegalStateException("start == workloads.length");
            } else if (totalWorkload(start, workload.length, normalizedWordloads) <= loadUpperLimit) {
                int[] newPack = new int[splits.length + 1];
                System.arraycopy(splits, 0, newPack, 0, splits.length);
                newPack[newPack.length - 1] = workload.length - start;
                packs.add(newPack);
            }
            return;
        }
        int[] newSplits = new int[splits.length + 1];
        System.arraycopy(splits, 0, newSplits, 0, splits.length);
        for (int i = start + 1; i < workload.length; i++) {
            if (totalWorkload(start, i, normalizedWordloads) > loadUpperLimit) {
                break;
            }
            newSplits[newSplits.length - 1] = i - start;
            genPacks(i, newSplits, part - 1, totalNumPartitions, loadUpperLimit, normalizedWordloads, packs);
        }
    }

    private double totalWorkload(int wStart, int wEnd, double[] normalizedWordloads) {
        double sum = 0;
        for (int i = wStart; i < wEnd; i++) {
            sum += normalizedWordloads[i];
        }
        return sum;
    }

    private double packGain(PackCalculator.Range[] pack1, PackCalculator.Range[] pack2, KuhnMunkres kmAlg) {
        double[][] weights = new double[pack1.length][pack2.length];
        for (int i = 0; i < pack1.length; i++) {
            for (int j = 0; j < pack2.length; j++) {
                weights[i][j] = overlap(pack1[i], pack2[j]);
            }
        }
        //System.out.println(Arrays.toString(pack1) + " " +Arrays.toString(pack2) );
        double[] maxWeight = new double[1];
        kmAlg.getMaxBipartie(weights, maxWeight);
        return maxWeight[0];
    }

    private double overlap(PackCalculator.Range r1, PackCalculator.Range r2) {
        if (r1.end < r2.start || r1.start > r2.end) {
            return 0;
        } else if (r1.start <= r2.start && r1.end >= r2.start) {
            return IntStream.rangeClosed(r2.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        } else if (r1.start >= r2.start && r1.start <= r2.end) {
            return IntStream.rangeClosed(r1.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        }
        return 0;
    }

}
