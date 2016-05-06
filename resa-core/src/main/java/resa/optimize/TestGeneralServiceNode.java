package resa.optimize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Tom.fu on 23/4/2014.
 */
public class TestGeneralServiceNode extends GeneralServiceNode{

    private static final Logger LOG = LoggerFactory.getLogger(TestGeneralServiceNode.class);

    public List<ExecServiceNode> execServiceNodeList;

    public TestGeneralServiceNode(
            String componentID,
            int executorNumber,
            double compSampleRate,
            double avgSendQueueLength,
            double avgRecvQueueLength,
            double avgServTimeHis,
            double scvServTimeHis,
            double numCompleteTuples,
            double sumDurationSeconds,
            double tupleCompleteRate,
            double lambda,
            double lambdaByInterArrival,
            double interArrivalScv,
            double exArrivalRate,
            double exArrivalRateByInterArrival,
            List<ExecServiceNode> execServiceNodeList) {
            super(componentID, executorNumber, compSampleRate, avgSendQueueLength, avgRecvQueueLength,
                    avgServTimeHis, scvServTimeHis, numCompleteTuples, sumDurationSeconds, tupleCompleteRate,
                    lambda, lambdaByInterArrival, interArrivalScv, exArrivalRate, exArrivalRateByInterArrival);
            this.execServiceNodeList = execServiceNodeList;
    }

    @Override
    public String toString() {
//        return String.format(
//                "Component(ID, eNum):(%s,%d), tupleProcCnt: %.1f, sumMeasuredDur: %.1f, sampleRate: %.1f, tupleProcRate: %.3f, " +
//                        "avgSendQLen: %.1f, avgRecvQLen: %.1f, avgServTimeMS: %.3f, scvServTime: %.3f, mu: %.3f, " +
//                        "arrRateHis: %.3f, arrRateBIA: %.3f, interArrivalScv: %.3f, " +
//                        "ratio: %.3f, ratioBIA: %.3f, rho: %.3f, rhoBIA: %.3f",
//                componentID, executorNumber, numCompleteTuples, sumDurationSeconds, compSampleRate, tupleCompleteRate,
//                avgSendQueueLength, avgRecvQueueLength, avgServTimeHis, scvServTimeHis, mu,
//                lambda, lambdaByInterArrival, interArrivalScv,
//                ratio, ratioByInterArrival, rho, rhoBIA);

        return String.format(
                "(ID, eNum):(%s,%d), ProcRate: %.3f, avgSTime: %.3f, scvSTime: %.3f, mu: %.3f, ProcCnt: %.1f, Dur: %.1f, sample: %.1f, SQLen: %.1f, RQLen: %.1f, " +
                "-----> arrRate: %.3f, arrRateBIA: %.3f, arrScv: %.3f, ratio: %.3f, ratioBIA: %.3f, rho: %.3f, rhoBIA: %.3f",
                componentID, executorNumber, tupleCompleteRate, avgServTimeHis, scvServTimeHis, mu,
                numCompleteTuples, sumDurationSeconds, compSampleRate, avgSendQueueLength, avgRecvQueueLength,
                lambda, lambdaByInterArrival, interArrivalScv, ratio, ratioByInterArrival, rho, rhoBIA);
    }
}
