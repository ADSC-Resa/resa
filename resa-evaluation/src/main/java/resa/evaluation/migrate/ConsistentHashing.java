package resa.evaluation.migrate;

import java.util.*;

/**
 * Created by ding on 15/1/19.
 */
public class ConsistentHashing {

    public static class Result {
        public final List<int[]> tasks;
        public final double cost;
        public final int[][] matching;

        public Result(List<int[]> tasks, double cost, int[][] matching) {
            this.tasks = tasks;
            this.cost = cost;
            this.matching = matching;
        }
    }

    public static Result calc(double[] dataSizes, List<int[]> currAlloc, int newSize) {
        if (newSize == currAlloc.size()) {
            throw new IllegalArgumentException("size equal");
        }
        return newSize > currAlloc.size() ? inc(dataSizes, currAlloc, newSize) : dec(dataSizes, currAlloc, newSize);
    }

    private static Result dec(double[] dataSizes, List<int[]> currAlloc, int newSize) {
        List<int[]> ret = new ArrayList<>(currAlloc);
        List<Integer> tasks2Move = new ArrayList<>();
        currAlloc.stream()
                .map(alloc -> new AllocDataSize(alloc, Arrays.stream(alloc).mapToDouble(i -> dataSizes[i]).sum()))
                .sorted().limit(currAlloc.size() - newSize)
                .forEach(allocDataSize -> {
                    ret.remove(allocDataSize.alloc);
                    Arrays.stream(allocDataSize.alloc).forEach(tasks2Move::add);
                });
        int avgAddSize = tasks2Move.size() / newSize, k = tasks2Move.size() % newSize, pos = 0;
        int[][] matching = new int[newSize][2];
        for (int j = 0; j < newSize; j++) {
            int newAllocSize = ret.get(j).length + avgAddSize;
            if (j < k) {
                newAllocSize++;
            }
            int[] newAlloc = new int[newAllocSize];
            System.arraycopy(ret.get(j), 0, newAlloc, 0, ret.get(j).length);
            for (int l = ret.get(j).length; l < newAllocSize; l++) {
                newAlloc[l] = tasks2Move.get(pos++);
            }
            matching[j][0] = j;
            matching[j][1] = currAlloc.indexOf(ret.get(j));
            ret.set(j, newAlloc);
        }
        return new Result(ret, tasks2Move.stream().mapToDouble(task -> dataSizes[task]).sum(), matching);
    }

    private static class AllocDataSize implements Comparable<AllocDataSize> {
        final int[] alloc;
        final double dataSize;

        private AllocDataSize(int[] alloc, double dataSize) {
            this.alloc = alloc;
            this.dataSize = dataSize;
        }

        @Override
        public int compareTo(AllocDataSize o) {
            return Double.compare(dataSize, o.dataSize);
        }
    }

    private static Result inc(double[] dataSizes, List<int[]> currAlloc, int newSize) {
        Map<int[], Integer> posMap = new HashMap<>();
        for (int i = 0; i < currAlloc.size(); i++) {
            posMap.put(currAlloc.get(i), i);
        }
        Collections.sort(currAlloc, (alloc1, alloc2) -> Integer.compare(alloc2.length, alloc1.length));
        int avgSize = dataSizes.length / newSize;
        int[] task2Move = new int[avgSize * (newSize - currAlloc.size())];
        List<int[]> ret = new ArrayList<>(newSize);
        int pos = 0;
        int[][] matching = new int[currAlloc.size()][2];
        for (int i = 0; i < currAlloc.size(); i++) {
            int[] tmp = Arrays.stream(currAlloc.get(i)).mapToObj(Integer::valueOf)
                    .sorted((i1, i2) -> Double.compare(dataSizes[i1], dataSizes[i2]))
                    .mapToInt(Integer::intValue).toArray();
            int cnt = task2Move.length / currAlloc.size();
            if (i < (task2Move.length % currAlloc.size())) {
                cnt++;
            }
            for (int j = 0; j < cnt; j++) {
                task2Move[pos++] = tmp[j];
            }
            ret.add(Arrays.copyOfRange(tmp, cnt, tmp.length));
            matching[i][0] = i;
            matching[i][1] = posMap.get(currAlloc.get(i));
        }
        for (int i = 0; i < newSize - currAlloc.size(); i++) {
            ret.add(Arrays.copyOfRange(task2Move, avgSize * i, avgSize * (i + 1)));
        }
        return new Result(ret, Arrays.stream(task2Move).mapToDouble(task -> dataSizes[task]).sum(), matching);
    }


}
