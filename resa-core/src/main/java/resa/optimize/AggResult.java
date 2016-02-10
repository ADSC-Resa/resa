package resa.optimize;

import java.util.Objects;

/**
 * Created by Tom.fu on 16/4/2014.
 * Modified by Tom Fu on Feb-10-2016,
 */
public class AggResult implements Cloneable {

    /**
     * The measured duration is in milliseconds
     */
    protected long duration = 0;
    protected QueueAggResult sendQueueResult = new QueueAggResult();
    protected QueueAggResult recvQueueResult = new QueueAggResult();

    /**
     * Vertical Combine is used for combine executor-level results to component-level results
     * At this implementation, vertical combine and horizontal combine are the same.
     * @param dest
     * @param data
     * @param <T>
     * @return
     */
    public static <T extends AggResult> T getVerticalCombinedResult(T dest, Iterable<AggResult> data) {
        data.forEach(dest::add);
        return dest;
    }

    /**
     * Horizontal Combine is used for combine results stored in the historical window, window-based combination
     * At this implementation, vertical combine and horizontal combine are the same.
     * @param dest
     * @param data
     * @param <T>
     * @return
     */
    public static <T extends AggResult> T getHorizontalCombinedResult(T dest, Iterable<AggResult> data) {
        data.forEach(dest::add);
        return dest;
    }

    public void add(AggResult r) {
        //CMVAdd will check null or not, no need to check here.
        //Objects.requireNonNull(r, "input AggResult cannot null");
        //Objects.requireNonNull(r, "input AggResult cannot null");
        this.duration += r.duration;
        this.sendQueueResult.add(r.sendQueueResult);
        this.recvQueueResult.add(r.recvQueueResult);
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public void addDuration(long duration) {
        this.duration += duration;
    }

    /**
     * The original duration and measured result in millisecond
     * @return the measured duration in MilliSecond
     */
    public long getDurationMilliSeconds(){
        return this.duration;
    }

    /**
     * The original duration and measured result in millisecond
     * @return this method return the measured duration in unit of seconds
     */
    public double getDurationSeconds(){
        return this.duration / 1000.0;
    }

    public double getArrivalRatePerSec() {
        return recvQueueResult.getArrivalCount() * 1000.0 / (double) duration;
    }

    public double getDepartureRatePerSec(){
        return sendQueueResult.getArrivalCount() * 1000.0 / (double) duration;
    }

    public QueueAggResult getSendQueueResult() {
        return sendQueueResult;
    }

    public QueueAggResult getRecvQueueResult() {
        return recvQueueResult;
    }
}
