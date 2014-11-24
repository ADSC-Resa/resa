package resa.optimize;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-5-6.
 */
public class SpoutAggResult extends AggResult {

    private Map<String, CntMeanVar> completedLatency = new HashMap<>();

    public Map<String, CntMeanVar> getCompletedLatency() {
        return completedLatency;
    }

    public CntMeanVar getCombinedCompletedLatency() {
        CntMeanVar retVal = new CntMeanVar();
        completedLatency.values().stream().forEach(retVal::addCMV);
        return retVal;
    }

    @Override
    public void add(AggResult r) {
        super.add(r);
        ((SpoutAggResult) r).completedLatency.forEach((s, cntMeanVar) ->
                this.completedLatency.computeIfAbsent(s, (k) -> new CntMeanVar()).addCMV(cntMeanVar));
    }
}
