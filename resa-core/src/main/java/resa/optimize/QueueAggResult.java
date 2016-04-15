package resa.optimize;

/**
 * Created by ding on 14-5-6.
 * Modified by Tom Fu on Feb-10-2016, queue related metrics modified by Ding, where we consider to measure
 * Tuple inter-arrival times at each executor queue (properly sampled)
 * This sample mechanism is shared by the original queue length measurement scheme.
 * Therefore, in the current implementation, the totalSampleCount is used by three metrics (avg QLen, interarrivalSum, interarrivalSum2)
 * In this way, we can calculate the 1st and 2nd moment of inter-arrival times
 */
public class QueueAggResult implements Cloneable {

    private long arrivalCount;
    private long totalQueueLength;
    private int totalSampleCount;
    private double arrIntervalSum;
    private double arrIntervalSum2;

    public QueueAggResult(long arrivalCount, long totalQueueLength, int totalSampleCount,
                          double arrIntervalSum, double arrIntervalSum2) {
        this.arrivalCount = arrivalCount;
        this.totalQueueLength = totalQueueLength;
        this.totalSampleCount = totalSampleCount;
        this.arrIntervalSum = arrIntervalSum;
        this.arrIntervalSum2 = arrIntervalSum2;
    }

    public QueueAggResult() {
        this(0, 0, 0, 0, 0);
    }

    public double getAvgQueueLength() {
        return totalSampleCount > 0 ? (double) totalQueueLength / (double) totalSampleCount : 0.0;
    }

    public double getAvgInterArrivalTimesMilliSecond() {
        return totalSampleCount > 0 ? arrIntervalSum / (double) totalSampleCount : 0.0;
    }

    public double getAvgArrivalRatePerSecond() {
        return totalSampleCount > 0 ? (double) totalSampleCount * 1000.0 / arrIntervalSum : 0.0;
    }

    ///Square coefficient of variation (Scv), scv(X) = Var(X) / [E(X)*E(X)] = (E(X_2) / [E(X)*E(X)]) - 1.0;
    public double getScvInterArrivalTimes() {
        if (totalSampleCount > 0) {
            return (double) totalSampleCount * arrIntervalSum2 / (arrIntervalSum * arrIntervalSum);
        } else {
            return 0.0;
        }
    }

    public long getArrivalCount() {
        return arrivalCount;
    }

    public long getTotalQueueLength() {
        return totalQueueLength;
    }

    public double getArrIntervalSum() {
        return arrIntervalSum;
    }

    public double getArrIntervalSum2() {
        return arrIntervalSum2;
    }

    public int getTotalSampleCount() {
        return totalSampleCount;
    }

    public void add(QueueAggResult result) {
        this.arrivalCount += result.arrivalCount;
        this.totalQueueLength += result.totalQueueLength;
        this.totalSampleCount += result.totalSampleCount;
        this.arrIntervalSum += result.arrIntervalSum;
        this.arrIntervalSum2 += result.arrIntervalSum2;
    }

    public void add(long arrivalCount, long totalQueueLength, int totalSampleCount,
                    double arrIntervalSum, double arrIntervalSum2) {
        this.arrivalCount += arrivalCount;
        this.totalQueueLength += totalQueueLength;
        this.totalSampleCount += totalSampleCount;
        this.arrIntervalSum += arrIntervalSum;
        this.arrIntervalSum2 += arrIntervalSum2;
    }

    @Override
    public String toString() {
        return String.format("arrCount: %d, totalQLen: %d, totalSamCnt: %d, arrIntervalSum: %.5f, arrIntervalSum2: %.5f",
                arrivalCount, totalQueueLength, totalSampleCount, arrIntervalSum, arrIntervalSum2);
    }
}
