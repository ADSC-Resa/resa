package resa.evaluation.migrate;

import java.util.List;

/**
 * Created by ding on 15/10/19.
 */
public class MigPlan {

    public final List<int[]> tasks;
    public final double cost;
    public final int[][] matching;

    public MigPlan(List<int[]> tasks, double cost, int[][] matching) {
        this.tasks = tasks;
        this.cost = cost;
        this.matching = matching;
    }
}
