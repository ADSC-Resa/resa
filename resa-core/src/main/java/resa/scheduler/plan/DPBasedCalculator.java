package resa.scheduler.plan;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-6-27.
 */
public class DPBasedCalculator extends PackCalculator {

    private Map<String, Pack> cache;

    protected Pack calcPack() {
        //init buffer
        cache = new HashMap<>();
        int[] packSegments = new int[srcPacks.length * 2];
        for (int i = 0; i < srcPacks.length; i++) {
            packSegments[2 * i] = 0;
            packSegments[2 * i + 1] = srcPacks[i].getKey().length;
        }
        Pack p = getCachedOrCompute(0, normalizedWordloads.length, packSize, packSegments);
        // release memory
        cache = null;
        return p;
    }

    private Pack getCachedOrCompute(int wStart, int wEnd, int numPartition, int[] packSegments) {
        if (wEnd - wStart < numPartition || numPartition == 0) {
            throw new IllegalStateException("start=" + wStart + ", end=" + wEnd + ", numPartition=" + numPartition);
        }
        String key = wStart + "-" + wEnd + "-" + numPartition + "@" + Arrays.toString(packSegments);
        Pack p = cache.get(key);
        if (p == null) {
            cache.put(key, (p = compute(wStart, wEnd, numPartition, packSegments)));
        }
        return p;
    }

    private Pack compute(int wStart, int wEnd, int numPartition, int[] packSegments) {
        if (numPartition == 1) {
            return totalWorkload(wStart, wEnd) > loadUpperLimit ? INFEASIBLE :
                    new Pack(new int[]{wEnd - wStart}, gain(new Range(wStart, wEnd - 1), packSegments));
        }
        int split = wStart + 1;
        double gain = Double.MIN_VALUE;
        Pack rightPack = INFEASIBLE;
        int[] tmpSegments = new int[packSegments.length];
        int[] newPos = new int[packSegments.length];
        for (int i = split; i < wEnd; i++) {
            if (totalWorkload(wStart, i) > loadUpperLimit || wEnd - i < numPartition - 1) {
                break;
            }
            if (totalWorkload(i, wEnd) > (numPartition - 1) * loadUpperLimit){
                continue;
            }
            for (int j = 0; j < srcPacks.length; j++) {
                Range[] srcPack = srcPacks[j].getKey();
                int startPack = findPack(srcPack, i);
                int stopPack = srcPack[startPack].start < i ? startPack + 1 : startPack;
                startPack = Math.max(startPack, packSegments[j * 2]);
                stopPack = Math.min(stopPack, packSegments[j * 2 + 1]);
                if (startPack == stopPack) {
                    newPos[j * 2] = startPack;
                    newPos[j * 2 + 1] = startPack;
                } else if (startPack == stopPack - 1) {
                    newPos[j * 2] = startPack;
                    newPos[j * 2 + 1] = stopPack;
                } else {
                    throw new IllegalStateException("for package " + j + ", start=" + startPack + ", stop=" + stopPack);
                }
            }
            long tryTimes = 1 << srcPacks.length;
            for (long j = 0; j < tryTimes; j++) {
                // go right
                for (int k = 0; k < srcPacks.length; k++) {
                    if ((j & (1L << k)) > 0) {
                        tmpSegments[k * 2] = newPos[k * 2 + 1];
                    } else {
                        tmpSegments[k * 2] = newPos[k * 2];
                    }
                    tmpSegments[k * 2 + 1] = packSegments[k * 2 + 1];
                }
                Pack right = getCachedOrCompute(i, wEnd, numPartition - 1, tmpSegments);
                if (right == INFEASIBLE) {
                    continue;
                }
                // go left
                for (int k = 0; k < srcPacks.length; k++) {
                    if ((j & (1L << k)) > 0) {
                        tmpSegments[k * 2 + 1] = newPos[k * 2 + 1];
                    } else {
                        tmpSegments[k * 2 + 1] = newPos[k * 2];
                    }
                    tmpSegments[k * 2] = packSegments[k * 2];
                }
                Pack left = getCachedOrCompute(wStart, i, 1, tmpSegments);
                if (left != INFEASIBLE) {
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

    private double gain(Range newPacks, int[] packSegments) {
        double sum = 0;
        for (int i = 0; i < srcPacks.length; i++) {
            double gain = 0;
            Range[] srcPack = srcPacks[i].getKey();
            for (int j = packSegments[i * 2]; j < packSegments[i * 2 + 1]; j++) {
                gain = Math.max(gain, overlap(newPacks, srcPack[j]));
            }
            sum += (gain * srcPacks[i].getValue());
        }
        return sum;
    }


    private int findPack(Range[] srcPack, int v) {
        for (int i = 0; i < srcPack.length; i++) {
            if (srcPack[i].end >= v) {
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

    private double overlap(Range r1, Range r2) {
        if (r1.start > r2.end || r1.end < r2.start) {
            return 0;
        } else if (r1.start <= r2.start && r1.end >= r2.start) {
            return IntStream.rangeClosed(r2.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        } else if (r1.start >= r2.start && r1.start <= r2.end) {
            return IntStream.rangeClosed(r1.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        }
        return 0;
    }

}
