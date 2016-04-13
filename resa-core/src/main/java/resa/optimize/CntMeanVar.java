package resa.optimize;

import java.util.Objects;

/**
 * Created by Tom.fu on 16/4/2014.
 */
public class CntMeanVar {
    private long count = 0;
    private double sum = 0;
    private double squareSum = 0;

    ///Add one measured valued
    void addOneNumber(double num) {
        count++;
        sum += num;
        squareSum += (num * num);
    }

    ///Add aggregated measure values in one window
    void addAggWin(int aggCount, double aggSum, double aggSquareSum) {
        count += aggCount;
        sum += aggSum;
        squareSum += aggSquareSum;
    }

    void addCMV(CntMeanVar cmv) {
        ///cmv should not be null here
        Objects.requireNonNull(cmv);

        if (cmv != null) {
            count += cmv.count;
            sum += cmv.sum;
            squareSum += cmv.squareSum;
        }
    }

    void clear() {
        count = 0;
        sum = 0;
        squareSum = 0;
    }

    long getCount() {
        return count;
    }

    double getSum() {
        return sum;
    }

    double getSquareSum() {
        return squareSum;
    }

    double getAvg() {
        return count > 0 ? sum / (double) count : 0.0;
    }

    double getAvg2() {
        return count > 0 ? squareSum / (double) count : 0.0;
    }

    double getVar() {
        return count > 0 ? getAvg2() - getAvg() * getAvg() : 0.0;
    }

    double getStd() {
        return count > 0 ? Math.sqrt(getVar()) : 0.0;
    }

    ///Square coefficient of variation (Scv), scv(X) = Var(X) / [E(X)*E(X)] = (E(X_2) / [E(X)*E(X)]) - 1.0;
    double getScv() {
        return count > 0 ? (getAvg2() / (getAvg() * getAvg()) - 1.0) : 0.0;
    }

    ///Make adjustment on Var(X) when X are samples from the original distribution
    double getScvAdjust() {
        return count > 1 ? getScv() * count / (count - 1) : getScv();
    }

    String toCMVString() {
        return "Count: " + getCount()
                + String.format(", sum: %.2f", getSum())
                + String.format(", avg: %.5f", getAvg())
                + String.format(", var: %.5f", getVar())
                + String.format(", scv: %.5f", getScv());
    }

    String toCMVStringShort() {
        return "cnt:" + getCount()
                + String.format(",avg:%.2f", getAvg())
                + String.format(",scv:%.2f", getScv());
    }
}
