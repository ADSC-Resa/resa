package resa.scheduler.plan;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by ding on 14-6-27.
 */
public class FastCalculator extends PackCalculator {

    private KuhnMunkres kmAlg;
    private double gain;

    @Override
    protected Pack calcPack() {
        Range[] retPack = convertPack(PackingAlg.calc(this.workloads, this.packSize));
        int maxPackSize = Stream.of(srcPacks).map(Map.Entry::getKey).mapToInt(pack -> pack.length).max().getAsInt();
        kmAlg = new KuhnMunkres(Math.max(retPack.length, maxPackSize));
        gain = totalGain(retPack);
        Range[] buf = new Range[retPack.length];
        while (true) {
            int packToAdj = -1, newEnd = -1;
            for (int i = 0; i < retPack.length - 1; i++) {
                int point = retPack[i].end;
                // step forward
                System.arraycopy(retPack, 0, buf, 0, buf.length);
                int p = stepForward(buf, i);
                if (p != point) {
                    packToAdj = i;
                    newEnd = p;
                }
                // step backward
                System.arraycopy(retPack, 0, buf, 0, buf.length);
                p = stepBackward(buf, i);
                if (p != point) {
                    packToAdj = i;
                    newEnd = p;
                }
            }
            // nothing to update
            if (packToAdj == -1) {
                break;
            } else {
                retPack[packToAdj] = new Range(retPack[packToAdj].start, newEnd);
                retPack[packToAdj + 1] = new Range(newEnd + 1, retPack[packToAdj + 1].end);
            }
        }
        kmAlg = null;
        return new Pack(convertPack(retPack), gain);
    }

    private int stepForward(Range[] orgPack, int point) {
        double workload = totalWorkload(orgPack[point].start, orgPack[point].end + 1);
        int newPos = orgPack[point].end;
        int j = orgPack[point].end + 1;
        final int upBound = orgPack[point + 1].end;
        while (j < upBound && (workload += normalizedWordloads[j]) <= loadUpperLimit) {
            orgPack[point] = new Range(orgPack[point].start, j);
            orgPack[point + 1] = new Range(j + 1, orgPack[point + 1].end);
            double g = totalGain(orgPack);
            if (g > gain) {
                gain = g;
                newPos = j;
            }
            j++;
        }
        return newPos;
    }

    private int stepBackward(Range[] orgPack, int point) {
        double workload = totalWorkload(orgPack[point + 1].start, orgPack[point + 1].end + 1);
        int newPos = orgPack[point].end;
        int j = orgPack[point].end - 1;
        final int lowBound = orgPack[point].start;
        while (j > lowBound && (workload += normalizedWordloads[j]) <= loadUpperLimit) {
            orgPack[point] = new Range(orgPack[point].start, j);
            orgPack[point + 1] = new Range(j + 1, orgPack[point + 1].end);
            double g = totalGain(orgPack);
            if (g > gain) {
                gain = g;
                newPos = j;
            }
            j--;
        }
        return newPos;
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
