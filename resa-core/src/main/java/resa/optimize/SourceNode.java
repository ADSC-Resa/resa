package resa.optimize;

/**
 * Created by Tom.fu on 23/5/2014.
 */
public class SourceNode {

    private double realLatencyMilliSecond;
    private double numCompleteTuples;
    private double sumDuration;
    private double tupleLeaveRateOnSQ;

    public SourceNode(double l, double n, double d, double r) {
        realLatencyMilliSecond = l;
        numCompleteTuples = n;
        sumDuration = d;
        tupleLeaveRateOnSQ = r;
    }

    public double getRealLatencyMilliSecond(){
        return realLatencyMilliSecond;
    }

    public double getRealLatencySec(){
        return realLatencyMilliSecond / 1000.0;
    }

    public double getNumCompleteTuples(){
        return numCompleteTuples;
    }

    ///in unit millisec
    public double getSumDurationMilliSec(){
        return sumDuration;
    }

    public double getSumDurationSec() {
        return sumDuration / 1000.0;
    }

    ///tuples per second
    public double getTupleCompleteRate(){
        return numCompleteTuples * 1000.0 / sumDuration;
    }

    public double getTupleLeaveRateOnSQ(){
        return tupleLeaveRateOnSQ;
    }
}
