package resa.optimize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tom.fu on 23/4/2014.
 */
public class ExecServiceNode {

    private static final Logger LOG = LoggerFactory.getLogger(ExecServiceNode.class);
    private double compSampleRate;

    private double avgSendQueueLength;
    private double avgRecvQueueLength;

    private double avgServTimeHis;
    private double scvServTimeHis;
    private double mu;

    private double numCompleteTuples;
    private double sumDurationSeconds;
    private double tupleCompleteRate;

    /*metrics on recv_queue*/
    private double lambda;
    private double lambdaByInterArrival;
    private double interArrivalScv;

    private double exArrivalRate;
    private double exArrivalRateByInterArrival;

    private double ratio;
    private double ratioByInterArrival;

    double rho = lambda / mu;
    double rhoBIA = lambdaByInterArrival / mu;

    public ExecServiceNode(
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
            double exArrivalRateByInterArrival) {
        this.compSampleRate = compSampleRate;
        this.avgSendQueueLength = avgSendQueueLength;
        this.avgRecvQueueLength = avgRecvQueueLength;
        this.avgServTimeHis = avgServTimeHis;
        this.scvServTimeHis = scvServTimeHis;
        this.numCompleteTuples = numCompleteTuples;
        this.sumDurationSeconds = sumDurationSeconds;
        this.tupleCompleteRate = tupleCompleteRate;
        this.lambda = lambda;
        this.lambdaByInterArrival = lambdaByInterArrival;
        this.interArrivalScv = interArrivalScv;
        this.exArrivalRate = exArrivalRate;
        this.exArrivalRateByInterArrival = exArrivalRateByInterArrival;

        this.mu = this.avgServTimeHis > 0.0 ? (1000.0 / this.avgServTimeHis) : Double.MAX_VALUE;
        this.ratio = this.exArrivalRate > 0.0 ? (this.lambda / this.exArrivalRate) : 0;
        this.ratioByInterArrival = this.exArrivalRateByInterArrival > 0.0 ? (this.lambdaByInterArrival / this.exArrivalRateByInterArrival) : 0;

        rho = this.lambda / mu;
        rhoBIA = this.lambdaByInterArrival / mu;

        LOG.info(toString());
    }

    @Override
    public String toString() {
        return String.format(
                "ProcRate: %.3f, avgSTime: %.3f, scvSTime: %.3f, mu: %.3f, ProcCnt: %.1f, Dur: %.1f, sample: %.1f, SQLen: %.1f, RQLen: %.1f, " +
                "-----> arrRate: %.3f, arrRateBIA: %.3f, arrScv: %.3f, ratio: %.3f, ratioBIA: %.3f, rho: %.3f, rhoBIA: %.3f",
                tupleCompleteRate, avgServTimeHis, scvServTimeHis, mu,
                numCompleteTuples, sumDurationSeconds, compSampleRate, avgSendQueueLength, avgRecvQueueLength,
                lambda, lambdaByInterArrival, interArrivalScv, ratio, ratioByInterArrival, rho, rhoBIA);
    }

    public double getCompSampleRate() {
        return compSampleRate;
    }

    public double getAvgSendQueueLength() {
        return avgSendQueueLength;
    }

    public double getAvgRecvQueueLength() {
        return avgRecvQueueLength;
    }

    public double getAvgServTimeHis() {
        return avgServTimeHis;
    }

    public double getScvServTimeHis() {
        return scvServTimeHis;
    }

    public double getNumCompleteTuples() {
        return numCompleteTuples;
    }

    public double getSumDurationSeconds() {
        return sumDurationSeconds;
    }

    public double getTupleCompleteRate() {
        return tupleCompleteRate;
    }

    public double getLambda() {
        return lambda;
    }

    public double getLambdaByInterArrival() {
        return lambdaByInterArrival;
    }

    public double getInterArrivalScv() {
        return interArrivalScv;
    }

    public double getExArrivalRate() {
        return exArrivalRate;
    }

    public double getExArrivalRateByInterArrival() {
        return exArrivalRateByInterArrival;
    }

    public double getMu() {
        return mu;
    }

    public double getRatio() {
        return ratio;
    }

    public double getRatioByInterArrival() {
        return ratioByInterArrival;
    }

    public double getRho() {
        return rho;
    }

    public double getRhoBIA() {
        return rhoBIA;
    }
}
