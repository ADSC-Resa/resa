package resa.optimize;

import backtype.storm.generated.StormTopology;
import resa.util.FixedSizeQueue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * Created by ding on 14-4-30.
 */
class AggregatedData {

    public AggregatedData(StormTopology rawTopology, int historySize) {
        this.rawTopology = rawTopology;
        this.historySize = historySize;
    }

    private StormTopology rawTopology;
    private int historySize;
    public final Map<String, Queue<AggResult>> compHistoryResults = new HashMap<>();

    public void putResult(String comp, AggResult[] exeAggResult) {
        AggResult aggResult = rawTopology.get_spouts().containsKey(comp) ? new SpoutAggResult() : new BoltAggResult();
        AggResult.getCombinedResult(aggResult, Arrays.asList(exeAggResult));
        compHistoryResults.computeIfAbsent(comp, (k) -> new FixedSizeQueue(historySize)).add(aggResult);
    }

    public void clear() {
        compHistoryResults.clear();
    }

}
