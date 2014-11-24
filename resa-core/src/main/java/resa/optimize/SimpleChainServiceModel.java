package resa.optimize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.util.ConfigUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by Tom.fu on 23/4/2014.
 * Chain topology and not tuple split
 */
public class SimpleChainServiceModel {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleChainServiceModel.class);

    /**
     * Like module A in our discussion
     *
     * @param components, the service node configuration, in this function, chain topology is assumed.
     * @param allocation, can be null input, in this case, directly return Infinity to indicator topology unstable
     * @return Double.MAX_VALUE when a) input allocation is null (i.e., system is unstable)
     * b) any one of the node is unstable (i.e., lambda/mu > 1, in which case, sn.estErlangT will be Double.MAX_VALUE)
     * else the validate estimated erlang service time.
     */
    public static double getErlangChainTopCompleteTime(Map<String, ServiceNode> components,
                                                       Map<String, Integer> allocation) {

        if (allocation == null) {
            return Double.MAX_VALUE;
        }
        double retVal = 0.0;
        for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
            String cid = e.getKey();
            ServiceNode sn = e.getValue();
            Integer serverCount = allocation.get(cid);
            // Objects.requireNonNull(serverCount, "No allocation entry find for this component" + cid);
            double est = sn.estErlangT(serverCount);
            if (est < Double.MAX_VALUE) {
                retVal += est;
            } else {
                return Double.MAX_VALUE;
            }
        }
        return retVal;
    }

    public static double getErlangChainTopCompleteTimeMilliSec(Map<String, ServiceNode> components,
                                                               Map<String, Integer> allocation) {
        double result = getErlangChainTopCompleteTime(components, allocation);
        return result < Double.MAX_VALUE ? (result * 1000.0) : Double.MAX_VALUE;
    }

    public static Map<String, Integer> getAllocation(Map<String, ServiceNode> components, Map<String, Object> para) {
        Map<String, Integer> retVal = new HashMap<>();

        components.forEach((cid, sn) -> {
            int curr = ConfigUtil.getInt(para, cid, 0);
            retVal.put(cid, curr);
        });
        return retVal;
    }

    public static boolean checkStable(Map<String, ServiceNode> components, Map<String, Integer> allocation) {
        return components.entrySet().stream().map(e -> e.getValue().isStable(allocation.get(e.getKey())))
                .allMatch(Boolean.TRUE::equals);
    }

    public static int getTotalMinRequirement(Map<String, ServiceNode> components) {
        return components.values().stream().mapToInt(ServiceNode::getMinReqServerCount).sum();
    }

    public static void printAllocation(Map<String, Integer> allocation) {
        if (allocation == null) {
            LOG.warn("Null allocation input -> system is unstable.");
        } else {
            LOG.info("allocation->" + allocation);
        }
    }

    /**
     * @param components
     * @param totalResourceCount
     * @return null if a) minReq of any component is Integer.MAX_VALUE (invalid parameter mu = 0.0)
     * b) total minReq can not be satisfied (total minReq > totalResourceCount)
     * otherwise, the Map data structure.
     */
    public static Map<String, Integer> suggestAllocation(Map<String, ServiceNode> components, int totalResourceCount) {
        Map<String, Integer> retVal = components.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> e.getValue().getMinReqServerCount()));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.info("totalResourceCount: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = Double.MIN_VALUE;
                String maxDiffCid = null;

                for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
                    String cid = e.getKey();
                    ServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(cid);

                    double beforeAddT = sn.estErlangT(currentAllocated);
                    double afterAddT = sn.estErlangT(currentAllocated + 1);

                    double diff = beforeAddT - afterAddT;
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                LOG.info((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: "
                        + newAllocate);
            }
        } else {
            return null;
        }
        return retVal;
    }

    /**
     * Like Module A', input required QoS, output #threads required
     * Here we separate to two modules: first output allocation, then calculate total #threads included.
     * Caution all the computation involved is in second unit.
     *
     * @param components
     * @param maxAllowedCompleteTime, the unit here is second! consistent with function getErlangChainTopCompleteTime()
     * @param lowerBoundDelta,        this is to set the offset of the lowerBoundServiceTime, we require delta to be positive, and 0 as default.
     * @param adjRatio,               this is to adjust the estimated ErlangServiceTime to fit more closely to the real measured complte time
     * @return null if a) any service node is not in the valid state (mu = 0.0), this is not the case of rho > 1.0, just for checking mu
     * b) lowerBoundServiceTime > requiredQoS
     */
    public static Map<String, Integer> getMinReqServerAllocation(Map<String, ServiceNode> components,
                                                                 double maxAllowedCompleteTime,
                                                                 double lowerBoundDelta,
                                                                 double adjRatio) {
        double lowerBoundServiceTime = 0.0;
        int totalMinReq = 0;
        for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
            double mu = e.getValue().getMu();
            ///caution, the unit should be millisecond
            lowerBoundServiceTime += (1.0 / mu);
            totalMinReq += e.getValue().getMinReqServerCount();
        }

        Map<String, Integer> currAllocation = null;
        if (lowerBoundServiceTime + lowerBoundDelta < maxAllowedCompleteTime) {
            double currTime;
            do {
                currAllocation = suggestAllocation(components, totalMinReq);
                currTime = getErlangChainTopCompleteTime(components, currAllocation) * adjRatio;

                LOG.info("getMinReqServAllcQoS: " + maxAllowedCompleteTime * 1000.0 + ", currTime(ms): "
                        + currTime * 1000.0 / adjRatio + ", currAdj(ms): "
                        + currTime * 1000.0 + ", totalMinReqQoS: " + totalMinReq);

                totalMinReq++;
            } while (currTime > maxAllowedCompleteTime);
        }
        return currAllocation;
    }

    public static Map<String, Integer> getMinReqServerAllocation(Map<String, ServiceNode> components,
                                                                 double maxAllowedCompleteTime) {
        return getMinReqServerAllocation(components, maxAllowedCompleteTime, 0.0, 1.0);
    }

    public static Map<String, Integer> getMinReqServerAllocation(Map<String, ServiceNode> components,
                                                                 double maxAllowedCompleteTime, double adjRatio) {
        return getMinReqServerAllocation(components, maxAllowedCompleteTime, 0.0, adjRatio);
    }

    public static int totalServerCountInvolved(Map<String, Integer> allocation) {
        return Objects.requireNonNull(allocation).values().stream().mapToInt(i -> i).sum();
    }

    public static AllocResult checkOptimized(Map<String, ServiceNode> queueingNetwork, double realLatencyMilliSec,
                                                  double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                  int maxAvailable4Bolt) {

        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double estimatedLatencyMilliSec = getErlangChainTopCompleteTimeMilliSec(queueingNetwork, currBoltAllocation);

        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSec / estimatedLatencyMilliSec);
        LOG.info("estLatency(ms): " + estimatedLatencyMilliSec + ", realLatency(ms)" + realLatencyMilliSec + ", underEstRatio: " + underEstimateRatio);
        LOG.info("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocation(queueingNetwork, targetQoSMilliSec / 1000.0,
                underEstimateRatio);
        AllocResult.Status status = AllocResult.Status.FEASIBALE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.info("Find out best allocation given available executors.");
        Map<String, Integer> after = suggestAllocation(queueingNetwork, maxAvailable4Bolt);
        return new AllocResult(status, minReqAllocation, after);
    }

    /**
     * Like module B in our discussion
     *
     * @param components
     * @param conf
     */
    public static void checkOptimized(Map<String, ServiceNode> components, Map<String, Object> conf) {

        Map<String, Integer> curr = getAllocation(components, conf);

        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double estimatedLatencyMilliSec = getErlangChainTopCompleteTimeMilliSec(components, curr);
        double realLatencyMilliSec = ConfigUtil.getDouble(conf, "avgCompleteHisMilliSec", estimatedLatencyMilliSec);

        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSec / estimatedLatencyMilliSec);

        double targetQoSMilliSec = ConfigUtil.getDouble(conf, "QoS", 5000.0);
        boolean targetQoSSatisfied = estimatedLatencyMilliSec < targetQoSMilliSec;
        int currAllocationCount = totalServerCountInvolved(curr);

        LOG.info("estimated: " + estimatedLatencyMilliSec + ", estiQoSSatisfied: " + targetQoSSatisfied + ", real: "
                + realLatencyMilliSec + ", realQoSSatisfied: " + (realLatencyMilliSec < targetQoSMilliSec));

        Map<String, Integer> minReqAllocation = getMinReqServerAllocation(components, targetQoSMilliSec / 1000.0,
                underEstimateRatio);
        int minReqTotalServerCount = minReqAllocation == null ? Integer.MAX_VALUE :
                totalServerCountInvolved(minReqAllocation);
        double minReqQoSMilliSec = getErlangChainTopCompleteTimeMilliSec(components,
                minReqAllocation);
        double adjMinReqQoSMilliSec = getErlangChainTopCompleteTimeMilliSec(components, minReqAllocation) *
                underEstimateRatio;

        if (minReqAllocation == null) {
            LOG.info("Caution: Target QoS is problematic, can not be achieved!");
        } else {
            LOG.info("MinReqTotalServerCount: " + minReqTotalServerCount + ", minReqQoS: " + minReqQoSMilliSec);
            LOG.info("underEstimateRatio: " + underEstimateRatio + ", adjMinReqQoS: " + adjMinReqQoSMilliSec
                    + ", optAllo: ");
            printAllocation(minReqAllocation);
        }

        if (minReqAllocation != null) {
            int remainCount = minReqTotalServerCount - currAllocationCount;
            if (remainCount > 0) {
                LOG.info("Require " + remainCount + " additional threads!!!");
            } else {
                LOG.info("Rebalance the current to suggested");
                Map<String, Integer> after = suggestAllocation(components, currAllocationCount);
                LOG.info("---------------------- Current Allocation ----------------------");
                printAllocation(curr);
                LOG.info("---------------------- Suggested Allocation ----------------------");
                printAllocation(after);
            }
        } else {
            LOG.info("Caution: Target QoS can never be achieved!");
        }

    }
}
