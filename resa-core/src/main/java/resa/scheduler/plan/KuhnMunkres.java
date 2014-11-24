package resa.scheduler.plan;

import java.util.Arrays;

/**
 * Created by ding on 14-6-6.
 */
public class KuhnMunkres {
    private int maxN, n, lenX, lenY;
    private double[][] weights;
    private boolean[] visitX, visitY;
    private double[] lx, ly;
    private double[] slack;
    private int[] match;

    public KuhnMunkres(int maxN) {
        this.maxN = maxN;
        visitX = new boolean[maxN];
        visitY = new boolean[maxN];
        lx = new double[maxN];
        ly = new double[maxN];
        slack = new double[maxN];
        match = new int[maxN];
    }

    public int[][] getMaxBipartie(double weight[][], double[] result) {
        if (!preProcess(weight)) {
            throw new IllegalArgumentException("Data overflow, max num is " + maxN);
        }
        //initialize memo data for class
        //initialize label X and Y
        Arrays.fill(ly, 0);
        Arrays.fill(lx, 0);
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if (lx[i] < weights[i][j]) {
                    lx[i] = weights[i][j];
                }
            }
        }

        //find a match for each X point
        for (int u = 0; u < n; u++) {
            Arrays.fill(slack, 0x7fffffff);
            while (true) {
                Arrays.fill(visitX, false);
                Arrays.fill(visitY, false);
                if (findPath(u))   //if find it, go on to the next point
                    break;
                //otherwise update labels so that more edge will be added in
                double inc = 0x7fffffff;
                for (int v = 0; v < n; v++) {
                    if (!visitY[v] && slack[v] < inc) {
                        inc = slack[v];
                    }
                }
                for (int i = 0; i < n; i++) {
                    if (visitX[i]) {
                        lx[i] -= inc;
                    }
                    if (visitY[i]) {
                        ly[i] += inc;
                    }
                }
            }
        }
        result[0] = 0.0;
        for (int i = 0; i < n; i++) {
            if (match[i] >= 0) {
                result[0] += weights[match[i]][i];
            }
        }
        return matchResult();
    }

    public int[][] matchResult() {
        int len = Math.min(lenX, lenY);
        int[][] res = new int[len][2];
        int count = 0;
        for (int i = 0; i < lenY; i++) {
            if (match[i] >= 0 && match[i] < lenX) {
                res[count][0] = match[i];
                res[count++][1] = i;
            }
        }
        return res;
    }

    private boolean preProcess(double[][] weight) {
        if (weight == null) {
            return false;
        }
        lenX = weight.length;
        lenY = weight[0].length;
        if (lenX > maxN || lenY > maxN) {
            return false;
        }
        Arrays.fill(match, -1);
        n = Math.max(lenX, lenY);
        weights = new double[n][n];
        for (int i = 0; i < lenX; i++) {
            for (int j = 0; j < lenY; j++) {
                weights[i][j] = weight[i][j];
            }
        }
        return true;
    }

    private boolean findPath(int u) {
        visitX[u] = true;
        for (int v = 0; v < n; v++) {
            if (!visitY[v]) {
                double temp = lx[u] + ly[v] - weights[u][v];
                if (temp == 0.0) {
                    visitY[v] = true;
                    if (match[v] == -1 || findPath(match[v])) {
                        match[v] = u;
                        return true;
                    }
                } else {
                    slack[v] = Math.min(slack[v], temp);
                }
            }
        }
        return false;
    }
}
