package resa.optimize;

/**
 * Created by ding on 14-5-6.
 */
public class QueueAggResult implements Cloneable {

    private long arrivalCount;
    private long totalQueueLength;
    private int totalSampleCount;

    public QueueAggResult(long arrivalCount, long totalQueueLength, int totalSampleCount) {
        this.arrivalCount = arrivalCount;
        this.totalQueueLength = totalQueueLength;
        this.totalSampleCount = totalSampleCount;
    }

    public QueueAggResult() {
        this(0, 0, 0);
    }

    public double getAvgQueueLength() {
        return totalSampleCount > 0 ? (double) totalQueueLength / (double) totalSampleCount : 0.0;
    }

    public long getArrivalCount() {
        return arrivalCount;
    }

    public long getTotalQueueLength() {
        return totalQueueLength;
    }

    public int getTotalSampleCount() {
        return totalSampleCount;
    }

    public void add(QueueAggResult result) {
        this.arrivalCount += result.arrivalCount;
        this.totalQueueLength += result.totalQueueLength;
        this.totalSampleCount += result.totalSampleCount;
    }

    public void add(long arrivalCount, long totalQueueLength, int totalSampleCount) {
        this.arrivalCount += arrivalCount;
        this.totalQueueLength += totalQueueLength;
        this.totalSampleCount += totalSampleCount;
    }

    @Override
    public String toString() {
        return String.format("arrCount: %d, totalQLen: %d, totalSamCnt: %d", arrivalCount,
                totalQueueLength, totalSampleCount);
    }
}
