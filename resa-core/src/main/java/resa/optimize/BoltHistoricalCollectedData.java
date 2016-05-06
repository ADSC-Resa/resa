package resa.optimize;

import backtype.storm.generated.StormTopology;
import resa.util.FixedSizeQueue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.ObjDoubleConsumer;

/**
 * Created by ding on 14-4-30.
 * Modified by Tom Fu on Feb-10-2016,
 */
class BoltHistoricalCollectedData extends HistoricalCollectedData {

    public BoltHistoricalCollectedData(StormTopology rawTopology, int historySize) {
            super(rawTopology, historySize);
    }

    public final Map<String, Queue<Object[]>> compHistoryResults = new HashMap<>();

    @Override
    public void putResult(String comp, AggResult[] exeAggResult) {
        AggResult compAggResult  = new BoltAggResult();
        ///First aggregate executorResults to Component results, vertical combine.
        AggResult.getVerticalCombinedResult(compAggResult, Arrays.asList(exeAggResult));
        ///Next, add aggregated component results to history window

        AggResult[] execResults = new BoltAggResult[exeAggResult.length];
        for (int i = 0; i < execResults.length; i ++){
            execResults[i] = new BoltAggResult();
            AggResult.getVerticalCombinedResult(execResults[i], Arrays.asList(exeAggResult[i]));
        }

        compHistoryResults.computeIfAbsent(comp, (k) -> new FixedSizeQueue(historySize)).add(new Object[]{compAggResult, execResults} );
    }

    public void clear() {
        compHistoryResults.clear();
    }

}
