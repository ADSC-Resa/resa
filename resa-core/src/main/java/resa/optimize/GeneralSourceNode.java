package resa.optimize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tom.fu on 23/5/2014.
 */
public class GeneralSourceNode {

    private static final Logger LOG = LoggerFactory.getLogger(GeneralSourceNode.class);

    private String componentID;
    private int executorNumber;
    private double compSampleRate;

    private double avgSendQueueLength;
    private double avgRecvQueueLength;

    private double realLatencyMilliSeconds;
    private double scvRealLatency;

    private double numCompleteTuples;
    private double sumDurationSeconds;
    private double tupleCompleteRate;

    /*metrics on send_queue*/
    private double tupleEmitRateOnSQ;
    private double tupleEmitRateByInterArrival;
    private double tupleEmitScvByInterArrival;
    /*metrics on recv_queue*/
    private double exArrivalRate;
    private double exArrivalRateByInterArrival;
    private double exArrivalScvByInterArrival;

    public GeneralSourceNode(
            String componentID,
            int executorNumber,
            double compSampleRate,
            double avgSendQueueLength,
            double avgRecvQueueLength,
            double realLatencyMilliSeconds,
            double scvRealLatency,
            double numCompleteTuples,
            double sumDurationSeconds,
            double tupleCompleteRate,
            double tupleEmitRateOnSQ,
            double tupleEmitRateByInterArrival,
            double tupleEmitScvByInterArrival,
            double exArrivalRate,
            double exArrivalRateByInterArrival,
            double exArrivalScvByInterArrival) {
        this.componentID = componentID;
        this.executorNumber = executorNumber;
        this.compSampleRate = compSampleRate;
        this.avgSendQueueLength = avgSendQueueLength;
        this.avgRecvQueueLength = avgRecvQueueLength;
        this.realLatencyMilliSeconds = realLatencyMilliSeconds;
        this.scvRealLatency = scvRealLatency;
        this.numCompleteTuples = numCompleteTuples;
        this.sumDurationSeconds = sumDurationSeconds;
        this.tupleCompleteRate = tupleCompleteRate;
        this.tupleEmitRateOnSQ = tupleEmitRateOnSQ;
        this.tupleEmitRateByInterArrival = tupleEmitRateByInterArrival;
        this.tupleEmitScvByInterArrival = tupleEmitScvByInterArrival;
        this.exArrivalRate = exArrivalRate;
        this.exArrivalRateByInterArrival = exArrivalRateByInterArrival;
        this.exArrivalScvByInterArrival = exArrivalScvByInterArrival;

        LOG.info(toString());
    }

    @Override
    public String toString() {

        return String.format(
                "Component(ID, eNum):(%s,%d), tupleFinCnt: %d, sumMeasuredDur: %.1f, sampleRate: %.1f, tupleFinRate: %.3f, " +
                        "avgSendQLen: %.1f, avgRecvQLen: %.1f, avgCompleteHis: %.4f, scvCompleteHis: %.4f, " +
                        "tupleEmitRateOnSQ: %.4f, tupleEmitRateBIA: %.4f, tupleEmitScvBIA: %.4f, " +
                        "exArrivalRate: %.4f, exArrivalRateBIA: %.4f, exArrivalScvBIA: %.4f",
                componentID, executorNumber, numCompleteTuples, sumDurationSeconds, compSampleRate, tupleCompleteRate,
                avgSendQueueLength, avgRecvQueueLength, realLatencyMilliSeconds, scvRealLatency,
                tupleEmitRateOnSQ, tupleEmitRateByInterArrival, tupleEmitScvByInterArrival,
                exArrivalRate, exArrivalRateByInterArrival, exArrivalScvByInterArrival);
    }

    public String getComponentID() {
        return this.componentID;
    }

    public int getExecutorNumber() {
        return executorNumber;
    }

    public double getCompSampleRate() {
        return compSampleRate;
    }

    public double getAvgSendQueueLength(){
        return avgSendQueueLength;
    }

    public double getAvgRecvQueueLength(){
        return avgRecvQueueLength;
    }

    public double getRealLatencyMilliSeconds() {
        return realLatencyMilliSeconds;
    }

    public double getScvRealLatency(){
        return scvRealLatency;
    }

    public double getRealLatencySeconds() {
        return realLatencyMilliSeconds / 1000.0;
    }

    public double getNumCompleteTuples() {
        return numCompleteTuples;
    }

    public double getSumDurationMilliSeconds() {
        return sumDurationSeconds * 1000.0;
    }

    public double getSumDurationSeconds() {
        return sumDurationSeconds;
    }

    public double getTupleCompleteRate() {
        return tupleCompleteRate;
    }

    public double getTupleEmitRateOnSQ() {
        return tupleEmitRateOnSQ;
    }

    public double getTupleEmitRateByInterArrival() {
        return tupleEmitRateByInterArrival;
    }

    public double getTupleEmitScvByInterArrival() {
        return tupleEmitScvByInterArrival;
    }

    public double getExArrivalRate() {
        return exArrivalRate;
    }

    public double getExArrivalRateByInterArrival() {
        return exArrivalRateByInterArrival;
    }

    public double getExArrivalScvByInterArrival() {
        return exArrivalScvByInterArrival;
    }
}
