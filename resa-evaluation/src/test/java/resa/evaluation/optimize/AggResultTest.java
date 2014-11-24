package resa.evaluation.optimize;

import org.junit.Test;
import resa.optimize.AggResult;

import java.util.ArrayList;
import java.util.List;

public class AggResultTest {

    private AggResult aggResult = new AggResult();

    @Test
    public void testGetCombinedResult() throws Exception {

        AggResult srcAggResult = new AggResult();
        srcAggResult.recvQueueResult.add(1, 1, 1);
        srcAggResult.sendQueueResult.add(2, 2, 2);

        List<AggResult> aggResultList = new ArrayList<>();
        aggResultList.add(srcAggResult);

        srcAggResult = new AggResult();
        srcAggResult.recvQueueResult.add(1, 1, 1);
        srcAggResult.sendQueueResult.add(2, 2, 2);

        aggResultList.add(srcAggResult);

        AggResult.getCombinedResult(aggResult, aggResultList);

        System.out.println(aggResult.getRecvQueueResult().toString());
        System.out.println(aggResult.getSendQueueResult().toString());

    }
}