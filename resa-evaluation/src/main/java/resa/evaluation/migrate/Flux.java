package resa.evaluation.migrate;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by ding on 15/10/19.
 */
public class Flux {

    public static MigPlan calc(double[] dataSizes, double[] workloads, List<int[]> currAlloc, int newSize) {
        if (newSize == currAlloc.size()) {
            throw new IllegalArgumentException("size equal");
        }
        return newSize > currAlloc.size() ? inc(dataSizes, workloads, currAlloc, newSize)
                : dec(dataSizes, workloads, currAlloc, newSize);
    }

    private static MigPlan dec(double[] dataSizes, double[] workloads, List<int[]> currAlloc, int newSize) {
        Node[] nodes = currAlloc.stream().limit(newSize).map(alloc -> new Node(alloc, workloads)).toArray(Node[]::new);
        Set<Integer> toMoveTasks = new HashSet<>();
        for (int i = newSize; i < currAlloc.size(); i++) {
            int[] alloc = currAlloc.get(i);
            for (int j = 0; j < alloc.length; j++) {
                nodes[(i + j) % newSize].addTask(alloc[j]);
                toMoveTasks.add(alloc[j]);
            }
        }
        double dataToMove = toMoveTasks.stream().mapToDouble(t -> dataSizes[t]).sum()
                + adjust(nodes, dataSizes, toMoveTasks);
        return new MigPlan(convertNodes(nodes), dataToMove, null);
    }

    private static List<int[]> convertNodes(Node[] nodes) {
        return Arrays.stream(nodes).map(node -> node.alloc.stream().mapToInt(i -> i).toArray())
                .collect(Collectors.toList());
    }

    private static double adjust(Node[] nodes, double[] dataSizes, Set<Integer> excludeTasks) {
        double totalToMove = 0;
        while (true) {
            Arrays.sort(nodes);
            Integer taskToMove = nodes[nodes.length - 1].findBestTask(nodes[0].currWorkload);
            if (taskToMove == null) {
                break;
            }
            nodes[nodes.length - 1].takeTask(taskToMove);
            nodes[0].addTask(taskToMove);
            if (excludeTasks.add(taskToMove)) {
                totalToMove += dataSizes[taskToMove];
            }
        }
        return totalToMove;
    }

    private static MigPlan inc(double[] dataSizes, double[] workloads, List<int[]> currAlloc, int newSize) {
        Node[] nodes = new Node[newSize];
        int i = 0;
        for (int[] alloc : currAlloc) {
            nodes[i++] = new Node(alloc, workloads);
        }
        while (i < newSize) {
            nodes[i++] = new Node(new int[0], workloads);
        }
        double dataToMove = adjust(nodes, dataSizes, new HashSet<>());
        return new MigPlan(convertNodes(nodes), dataToMove, null);
    }


    private static class Node implements Comparable<Node> {
        List<Integer> alloc;
        double[] workloads;
        double currWorkload;

        public Node(int[] alloc, double[] workloads) {
            this.alloc = Arrays.stream(alloc).boxed().collect(Collectors.toList());
            this.workloads = workloads;
            currWorkload = Arrays.stream(alloc).mapToDouble(i -> workloads[i]).sum();
        }

        @Override
        public int compareTo(Node o) {
            return Double.compare(currWorkload, o.currWorkload);
        }

        public Integer findBestTask(double destWorkload) {
            int best = -1;
            double diff = currWorkload - destWorkload;
            for (int i = 0; i < alloc.size(); i++) {
                double currDiff = Math.abs((destWorkload + workloads[alloc.get(i)]) -
                        (currWorkload - workloads[alloc.get(i)]));
                if (diff > currDiff) {
                    best = i;
                    diff = currDiff;
                }
            }
            return best < 0 ? null : alloc.get(best);
        }

        public void takeTask(int task) {
            alloc.remove((Object) task);
            currWorkload -= workloads[task];
        }

        public void addTask(int task) {
            alloc.add(task);
            currWorkload += workloads[task];
        }
    }

}