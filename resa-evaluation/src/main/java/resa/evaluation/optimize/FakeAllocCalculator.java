package resa.evaluation.optimize;

import resa.optimize.AggResult;
import resa.optimize.AllocCalculator;
import resa.optimize.AllocResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-5-1.
 */
public class FakeAllocCalculator extends AllocCalculator {

    @Override
    public AllocResult calc(Map<String, AggResult[]> executorAggResults, int maxAvailableExecutors) {
        Map<String, Integer> ret = new HashMap<>(currAllocation);
        ArrayList<Map.Entry<String, Integer>> tmp = new ArrayList<>(ret.entrySet());
        Collections.shuffle(tmp);
        Map.Entry<String, Integer> entry = tmp.get(0);
        int old = entry.getValue();
        int newThreads = System.currentTimeMillis() % 2 == 0 ? old + 1 : old - 1;
        entry.setValue(newThreads);
        System.out.println(entry.getKey() + ": Old is " + old + ", new is " + newThreads);
        return new AllocResult(AllocResult.Status.FEASIBLE, ret, ret);
    }
}
