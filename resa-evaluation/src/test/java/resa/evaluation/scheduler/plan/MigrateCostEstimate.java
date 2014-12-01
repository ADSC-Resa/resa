package resa.evaluation.scheduler.plan;

import org.junit.Before;
import org.junit.Test;
import resa.migrate.plan.DPBasedCalculator;
import resa.migrate.plan.KuhnMunkres;
import resa.migrate.plan.PackCalculator;
import resa.migrate.plan.PackingAlg;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static resa.migrate.plan.PackCalculator.convertPack;

/**
 * Created by ding on 14-7-21.
 */
public class MigrateCostEstimate {

    private double[] dataSizes;
    private double[] workload;
    private double totalDataSize;
    private TreeMap<String, Double> migrationMetrics;
    private float ratio = 1.22f;
    private int[] statesChain = null;

    @Before
    public void init() throws Exception {
        workload = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/wc-workload-096.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        dataSizes = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/wc-data-sizes-096.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        totalDataSize = DoubleStream.of(dataSizes).sum();
        migrationMetrics = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/fp-metrics.txt")).stream()
                .map(s -> s.split(":")).collect(Collectors.toMap(strs -> strs[0] + "-" + strs[1],
                        strs -> Double.parseDouble(strs[2]), (v1, v2) -> {
                            throw new IllegalStateException();
                        }, TreeMap::new));
        Path chainFile = Paths.get("/tmp/chain.txt");
        if (Files.exists(chainFile)) {
//            statesChain = Files.lines(chainFile).map(String::trim).filter(s -> !s.isEmpty())
//                    .mapToInt(Integer::parseInt).toArray();
            statesChain = Stream.of(Files.readAllLines(chainFile).get(0).split(",")).map(String::trim)
                    .filter(s -> !s.isEmpty()).mapToInt(Integer::parseInt).toArray();
        }
    }

    private int getNextState(int curr) {
        Map.Entry<String, Double>[] states = migrationMetrics.subMap(curr + "-", curr + "~").entrySet()
                .toArray(new Map.Entry[0]);
        double sum = Stream.of(states).mapToDouble(e -> e.getValue()).sum();
        double rand = Math.random();
        double d = 0;
        for (int i = 0; i < states.length; i++) {
            d += (states[i].getValue() / sum);
            if (d >= rand) {
                return Integer.parseInt(states[i].getKey().split("-")[1]);
            }
        }
        throw new IllegalStateException();
    }

    private Map<Integer, Double> getTargetState(int curr) {
        Map<Integer, Double> states = migrationMetrics.subMap(curr + "-", curr + "~").entrySet().stream()
                .collect(Collectors.toMap(e -> Integer.parseInt(e.getKey().split("-")[1]), e -> e.getValue()));
        double sum = states.values().stream().mapToDouble(d -> d).sum();
        for (Map.Entry<Integer, Double> entry : states.entrySet()) {
            double newValue = entry.getValue() / sum;
            entry.setValue(newValue);
        }
        return states;
    }

    private Map<Integer, Double> getNeighState(int curr) {
        Map<Integer, Double> states = new HashMap<>();
        String currState = String.valueOf(curr);
        migrationMetrics.entrySet().stream().filter(e -> e.getKey().contains(currState)).forEach(e -> {
            String[] stateStrs = e.getKey().split("-");
            String nei = stateStrs[0].equals(currState) ? stateStrs[1] : stateStrs[0];
            states.compute(Integer.valueOf(nei), (k, v1) -> v1 == null ? e.getValue() : v1 + e.getValue());
        });
        double sum = states.values().stream().mapToDouble(d -> d).sum();
        for (Map.Entry<Integer, Double> entry : states.entrySet()) {
            double newValue = entry.getValue() / sum;
            entry.setValue(newValue);
        }
        return states;
    }


    @Test
    public void runTime1() {
        int[] states = {4, 6};
        int count = 10000;
        KuhnMunkres km = new KuhnMunkres(dataSizes.length);
        PackCalculator calculator = new DPBasedCalculator().setWorkloads(workload).setDataSizes(dataSizes)
                .setUpperLimitRatio(ratio);
        int[] srcPack = PackingAlg.calc(workload, states[0]);
        for (int i = 0; i < 1000; i++) {
            calculator.setSrcPack(srcPack).setTargetPackSize(states[1]).calc();
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            calculator.setSrcPack(srcPack).setTargetPackSize(states[1]).calc();
        }
        System.out.println("avg cost " + (System.currentTimeMillis() - start) / (count - 0.0) + " ms");
    }

    @Test
    public void runTime() {
        int[] states;
        int count = 10000;
        if (statesChain == null) {
            states = new int[count];
            states[0] = 8;
            for (int i = 1; i < states.length; i++) {
                states[i] = getNextState(states[i - 1]);
            }
        } else {
            states = statesChain;
            count = states.length;
        }
        System.out.println(IntStream.of(states).mapToObj(String::valueOf).collect(Collectors.joining(",")));
//        for (int i = 1; i < states.length; i++) {
//            System.out.print(getTargetState(states[i - 1]).get(states[i]));
//            System.out.print(",");
//        }
//        System.out.println();
        KuhnMunkres km = new KuhnMunkres(dataSizes.length);
        calcLocalOptimization(states, km);
        long start = System.currentTimeMillis();
        calcLocalOptimization(states, km);
        System.out.println("avg cost " + (System.currentTimeMillis() - start) / (count - 1.0) + " ms");
    }

    @Test
    public void compare() {
        int[] states;
        int count = 100;
        if (statesChain == null) {
            states = new int[count];
            states[0] = 8;
            for (int i = 1; i < states.length; i++) {
                states[i] = getNextState(states[i - 1]);
            }
        } else {
            states = statesChain;
            count = states.length;
        }
        System.out.println(IntStream.of(states).mapToObj(String::valueOf).collect(Collectors.joining(",")));
//        for (int i = 1; i < states.length; i++) {
//            System.out.print(getTargetState(states[i - 1]).get(states[i]));
//            System.out.print(",");
//        }
//        System.out.println();
        System.out.println("First pack:" + Arrays.toString(PackingAlg.calc(workload, states[0])));
        KuhnMunkres km = new KuhnMunkres(dataSizes.length);
        System.out.println("default: " + output(calcDefault(states, km), count - 1));
        System.out.println("local opt: " + output(calcLocalOptimization(states, km), count - 1));
//        System.out.println("global opt1: " + output(calcGlobalOptimization(states, km), count));
//        System.out.println("global opt2: " + output(calcGlobalOptimization2(states, 0.8f), count - 1));
//        System.out.println("global opt3: " + output(calcGlobalOptimization3(states, 0.8f), count - 1));
    }

    private String output(double toMove, int count) {
        toMove = toMove / count;
        return String.format("Avg to move %dbytes, total %dbytes, rate %f", (long) toMove, (long) totalDataSize,
                toMove / totalDataSize);
    }


    private double calcGlobalOptimization(int[] states, KuhnMunkres km) {
        Map<Integer, int[]> state2Pack = IntStream.of(states).distinct().boxed()
                .collect(Collectors.toMap(i -> i, i -> packAvg(workload.length, i)));
        double toMove = 0.0;
        for (int i = 1; i < states.length; i++) {
            PackCalculator.Range[] currPack = convertPack(state2Pack.get(states[i - 1]));
            calcBest1(states[i - 1], states[i], state2Pack);
            double remain = packGain(currPack, convertPack(state2Pack.get(states[i])), km);
//            System.out.printf("%.2f\n", (totalDataSize - remain) / 1024);
            toMove += (totalDataSize - remain);
//            System.out.println(IntStream.of(state2Pack.get(states[i])).mapToObj(String::valueOf)
//                    .collect(Collectors.joining(",")));
        }
        return toMove;
    }

    private void calcBest1(int currState, int nextStat, Map<Integer, int[]> state2Pack) {
        int[] states = state2Pack.keySet().stream().mapToInt(i -> i).toArray();
        Map<Integer, Double> gain = new TreeMap<>();
        PackCalculator calculator = new DPBasedCalculator().setWorkloads(workload).setDataSizes(dataSizes)
                .setUpperLimitRatio(ratio);
        //  Map<Integer, Double> newGain = new HashMap<>();
        int[] initState = Arrays.copyOf(state2Pack.get(currState), currState);
        int j;
        do {
            j = 0;
            for (int i = 0; i < states.length; i++) {
                Map<int[], Double> packs = new HashMap<>();
                getTargetState(states[i]).forEach((state, p) -> packs.put(state2Pack.get(state), p));
                if (states[i] == nextStat) {
//                    for (Map.Entry<int[], Double> entry : packs.entrySet()) {
//                        entry.setValue(entry.getValue() * 0.5);
//                    }
                    packs.put(initState, 1.01);
                }
                calculator.setSrcPacks(packs).setTargetPackSize(states[i]).calc();
                state2Pack.put(states[i], calculator.getPack());
                double g = calculator.gain();
                Double oldGain = gain.put(states[i], g);
                if (oldGain == null || Math.abs(g - oldGain) > 100) {
                    j++;
                }
            }
//            System.out.println(gain);
        } while (j > 0);
//        System.out.println("-----------");
    }

    private double calcGlobalOptimization3(int[] states, float gam) {
        Path valuesDir = Paths.get("/Volumes/Data/work/doctor/resa/migration/mdp/matrix/wc-new/"
                + String.format("%.2f", ratio));
        Map<Integer, float[]> state2Values = IntStream.of(states).distinct().boxed().collect(Collectors.toMap(i -> i,
                i -> readValues(valuesDir.resolve("values_" + i))));
        Map<Integer, int[][]> statePacks = IntStream.of(states).distinct().boxed().collect(Collectors.toMap(i -> i,
                i -> readPacks(valuesDir.resolve("state_" + i))));
        int[] srcPack = PackingAlg.calc(workload, states[0]);
        double toMove = 0.0;
        for (int i = 1; i < states.length; i++) {
            PackCalculator.Range[] currPack = convertPack(srcPack);
            int[] selected = selectTargetPack(currPack, statePacks.get(states[i]), state2Values.get(states[i]), gam);
            double remain = packGain(currPack, convertPack(selected), new KuhnMunkres(workload.length));
//            System.out.printf("%.2f\n", (totalDataSize - remain) / 1024);
            toMove += (totalDataSize - remain);
//            System.out.println(IntStream.of(selected).mapToObj(String::valueOf).collect(Collectors.joining(",")));
            srcPack = selected;
        }
        return toMove;
    }

    private double calcGlobalOptimization2(int[] states, float gam) {
        Path valuesDir = Paths.get("/Volumes/Data/work/doctor/resa/migration/mdp/matrix/"
                + String.format("%.2f-0.8", ratio));
        Map<Integer, float[]> state2Values = IntStream.of(states).distinct().boxed().collect(Collectors.toMap(i -> i,
                i -> readValues(valuesDir.resolve("values_" + i))));
        Map<Integer, int[][]> statePacks = IntStream.of(states).distinct().boxed().collect(Collectors.toMap(i -> i,
                i -> readPacks(valuesDir.resolve("state_" + i))));
        state2Values.forEach((state, values) -> {
            if (values.length != statePacks.get(state).length) {
                throw new IllegalStateException("values.length != statePacks.get(state).length");
            }
        });

        int[] srcPack = PackingAlg.calc(workload, states[0]);
        double toMove = 0.0;
        for (int i = 1; i < states.length; i++) {
            PackCalculator.Range[] currPack = convertPack(srcPack);
            int[] selected = selectTargetPack(currPack, statePacks.get(states[i]), state2Values.get(states[i]), gam);
            double remain = packGain(currPack, convertPack(selected), new KuhnMunkres(workload.length));
//            System.out.printf("%.2f\n", (totalDataSize - remain) / 1024);
            toMove += (totalDataSize - remain);
//            System.out.println(IntStream.of(selected).mapToObj(String::valueOf).collect(Collectors.joining(",")));
            srcPack = selected;
        }
        return toMove;
    }

    private int[] selectTargetPack(PackCalculator.Range[] srcPack, int[][] allTargetPacks, float[] costVector, float gam) {
//        int[] selected = null;
//        double minCost = Double.MAX_VALUE;
//        for (int i = 0; i < allTargetPacks.length; i++) {
//            double cost = costVector[i] + (totalDataSize - packGain(srcPack, convertPack(allTargetPacks[i]), km));
//            if (cost < minCost) {
//                minCost = cost;
//                selected = allTargetPacks[i];
//            }
//        }
        return IntStream.range(0, allTargetPacks.length).parallel().mapToObj(i -> new PackCost(gam * costVector[i]
                + (totalDataSize - packGain(srcPack, convertPack(allTargetPacks[i]), new KuhnMunkres(dataSizes.length))), allTargetPacks[i]))
                .min(Comparator.<PackCost>naturalOrder()).get().pack;
    }

    private static class PackCost implements Comparable<PackCost> {
        double cost;
        int[] pack;

        PackCost(double cost, int[] pack) {
            this.cost = cost;
            this.pack = pack;
        }

        @Override
        public int compareTo(PackCost o) {
            return Double.compare(cost, o.cost);
        }
    }

    private float[] readValues(Path file) {
        double[] tmp;
        try (Stream<String> lines = Files.lines(file)) {
            tmp = lines.mapToDouble(Double::parseDouble).toArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        float[] values = new float[tmp.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = (float) tmp[i];
        }
        return values;
    }

    private int[][] readPacks(Path file) {
        try (Stream<String> lines = Files.lines(file)) {
            return lines.map(s -> s.split(":")[1]).map(s -> s.split(",")).map(strArr -> Stream.of(strArr)
                    .mapToInt(Integer::parseInt).toArray()).toArray(int[][]::new);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private double calcLocalOptimization(int[] states, KuhnMunkres km) {
        PackCalculator calculator = new DPBasedCalculator().setWorkloads(workload).setDataSizes(dataSizes)
                .setUpperLimitRatio(ratio);
        double toMove = 0.0;
        int[] srcPack = PackingAlg.calc(workload, states[0]);
//        int[] srcPack = {8, 7, 8, 9, 7, 7, 10, 8};
        for (int i = 1; i < states.length; i++) {
            calculator.setSrcPack(srcPack).setTargetPackSize(states[i]).calc();
            int[] newPack = calculator.getPack();
            double remain = packGain(convertPack(srcPack), convertPack(newPack), km);
            toMove += (totalDataSize - remain);
//            System.out.printf("%.2f\n", (totalDataSize - remain) / 1024);
            srcPack = newPack;
//            System.out.println(IntStream.of(newPack).mapToObj(String::valueOf).collect(Collectors.joining(",")));
        }
        return toMove;
    }

    private double calcDefault(int[] states, KuhnMunkres km) {
        int[] srcPack = packAvg(workload.length, states[0]);
        double toMove = 0.0;
        for (int i = 1; i < states.length; i++) {
            int[] newPack = packAvg(workload.length, states[i]);
            double remain = packGain(convertPack(srcPack), convertPack(newPack), km);
            toMove += (totalDataSize - remain);
            srcPack = newPack;
        }
        return toMove;
    }

    private double packGain(PackCalculator.Range[] pack1, PackCalculator.Range[] pack2, KuhnMunkres kmAlg) {
        double[][] weights = new double[pack1.length][pack2.length];
        for (int i = 0; i < pack1.length; i++) {
            for (int j = 0; j < pack2.length; j++) {
                weights[i][j] = overlap(pack1[i], pack2[j]);
            }
        }
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


    private static int[] packAvg(int eleCount, int packCount) {
        int[] ret = new int[packCount];
        Arrays.fill(ret, eleCount / packCount);
        int k = eleCount % packCount;
        for (int i = 0; i < k; i++) {
            ret[i]++;
        }
        return ret;
    }

}
