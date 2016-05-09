package resa.optimize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.util.ConfigUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by Tom.fu on Feb-15-2016
 * Chain topology and not tuple split
 * TODO: the current implementation has adjustRatio, in future, we will design general regression model
 * TODO: or prediction method to replace this.
 * <p>
 * TODO: sn.getI2oRatio() can be INFINITY, i.e., special case, there is no input data
 * <p>
 * TODO: we have already tried in this version! when we calculate Var(X), where X are sampled results, Var(X) shall multiply n/(n-1), n is element counts of X.
 * The adjustment on Var(x) has been tested. The effect is too small (since n is quite large), therefore, we decide to ignore this.
 */
public class TestGeneralServiceModel {

    private static final Logger LOG = LoggerFactory.getLogger(TestGeneralServiceModel.class);

    public enum ServiceModelType {
        MMK(0), GGK_SimpleAppr(1), GGK_SimpleApprBIA(2), GGK_ComplexAppr(3), GGK_ComplexApprBIA(4), MMK_Ex(5), GGK_ComplexAppr_Ex(6);
        private static final int totalTypeCount = 7;
        //        The adjustment on Var(x) has been tested. The effect is too small (since n is quite large), therefore, we decide to ignore this.
//        MMK(0), GGK_SimpleAppr(1), GGK_SimpleAdj(2);
//        private static final int totalTypeCount = 3;
        private final int value;

        ServiceModelType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * We assume the stability check for each node is done beforehand!
     * Jackson OQN assumes all the arrival and departure are iid and exponential
     * <p>
     * Note, the return time unit is in Second!
     *
     * @param serviceNodes, the service node configuration, in this function, chain topology is assumed.
     * @param allocation,   the target allocation to be analyzed
     * @return here we should assume all the components are stable, the stability check shall be done outside this function
     */
    public static double getExpectedTotalSojournTimeForJacksonOQN(Map<String, GeneralServiceNode> serviceNodes, Map<String, Integer> allocation) {

        double retVal = 0.0;
        for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();
            GeneralServiceNode serviceNode = e.getValue();
            int serverCount = allocation.get(cid).intValue();

            double avgSojournTime = sojournTime_MMK(serviceNode.getLambda(), serviceNode.getMu(), serverCount);
            retVal += (avgSojournTime * serviceNode.getRatio());
        }
        return retVal;
    }

    /**
     * We assume the stability check for each node is done beforehand!
     * Jackson OQN assumes all the arrival and departure are iid and exponential
     * <p>
     * Note, the return time unit is in Second!
     *
     * @param serviceNodes, the service node configuration, in this function, chain topology is assumed.
     * @param allocation,   the target allocation to be analyzed
     * @return here we should assume all the components are stable, the stability check shall be done outside this function
     */
    public static double getExpectedTotalSojournTimeForJacksonOQN_exec(Map<String, GeneralServiceNode> serviceNodes, Map<String, Integer> allocation) {

        double retVal = 0.0;
        for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();

            TestGeneralServiceNode testGeneralServiceNode = (TestGeneralServiceNode) e.getValue();
            int serverCount = allocation.get(cid).intValue();
//            int maxIndex = testGeneralServiceNode.getMaxIndexByMMK();
            double maxAvgSojournTime = sojournTime_MMK(
                    testGeneralServiceNode.getLambda(),
                    testGeneralServiceNode.getMinMu(), serverCount);
            retVal += (maxAvgSojournTime * testGeneralServiceNode.getRatio());
        }
        return retVal;
    }

    /**
     * We assume the stability check for each node is done beforehand!
     * Jackson OQN assumes all the arrival and departure are iid and exponential
     * <p>
     * Note, the return time unit is in Second!
     *
     * @param serviceNodes, the service node configuration, in this function, chain topology is assumed.
     * @param allocation,   the target allocation to be analyzed
     * @return here we should assume all the components are stable, the stability check shall be done outside this function
     */
    public static double getExpectedTotalSojournTimeForJacksonOQN_execAvg(Map<String, GeneralServiceNode> serviceNodes, Map<String, Integer> allocation) {

        double retVal = 0.0;
        for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();

            TestGeneralServiceNode testGeneralServiceNode = (TestGeneralServiceNode) e.getValue();
            int serverCount = allocation.get(cid).intValue();
            double maxAvgSojournTime = 0;
            double totalLambda = 0;

            for (int i = 0; i < testGeneralServiceNode.execServiceNodeList.size(); i++) {
                totalLambda += testGeneralServiceNode.execServiceNodeList.get(i).getLambda();
                double avgSojournTime = sojournTime_MMK(
                        testGeneralServiceNode.execServiceNodeList.get(i).getLambda(),
                        testGeneralServiceNode.execServiceNodeList.get(i).getMu(), 1);
                maxAvgSojournTime += (avgSojournTime * testGeneralServiceNode.execServiceNodeList.get(i).getLambda());
            }

            retVal += (maxAvgSojournTime * testGeneralServiceNode.getRatio() / totalLambda);
        }
        return retVal;
    }

    /**
     * TODO: Caution! we have tried new calculation on Var(X), where X are sampled results, Var(X) shall multiply n/(n-1), n is element counts of X.
     * We assume the stability check for each node is done beforehand!
     * Only assume iid with general distribution on interarrival times and service times
     * apply G/G/k service model
     *
     * @param serviceNodes, the service node configuration, in this function, chain topology is assumed.
     * @param allocation,   the target allocation to be analyzed
     * @return here we should assume all the components are stable, the stability check shall be done outside this function
     */
    public static double getExpectedTotalSojournTimeForGeneralizedOQN_SimpleAppr(Map<String, GeneralServiceNode> serviceNodes, Map<String, Integer> allocation) {

        double retVal = 0.0;
        for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();
            GeneralServiceNode serviceNode = e.getValue();
            int serverCount = allocation.get(cid).intValue();
            double avgSojournTime = sojournTime_GGK_SimpleAppr(
                    serviceNode.getLambda(), serviceNode.getInterArrivalScv(), serviceNode.getMu(), serviceNode.getScvServTimeHis(), serverCount);
            retVal += (avgSojournTime * serviceNode.getRatio());
        }
        return retVal;
    }


    public static double getExpectedTotalSojournTimeForGeneralizedOQN_ComplexAppr(Map<String, GeneralServiceNode> serviceNodes, Map<String, Integer> allocation) {

        double retVal = 0.0;
        for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();
            GeneralServiceNode serviceNode = e.getValue();
            int serverCount = allocation.get(cid).intValue();
            double avgSojournTime = sojournTime_GGK_ComplexAppr(
                    serviceNode.getLambda(), serviceNode.getInterArrivalScv(), serviceNode.getMu(), serviceNode.getScvServTimeHis(), serverCount);
            retVal += (avgSojournTime * serviceNode.getRatio());
        }
        return retVal;
    }

    public static double getExpectedTotalSojournTimeForGeneralizedOQN_ComplexAppr_exec(Map<String, GeneralServiceNode> serviceNodes, Map<String, Integer> allocation) {

        double retVal = 0.0;
        for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();
            TestGeneralServiceNode testGeneralServiceNode = (TestGeneralServiceNode) e.getValue();
            int serverCount = allocation.get(cid).intValue();
//            int maxIndex = testGeneralServiceNode.getMaxIndexByGGK();
            double maxAvgSojournTime = sojournTime_GGK_ComplexAppr(
                    testGeneralServiceNode.getLambda(),
                    testGeneralServiceNode.getInterArrivalScv(),
                    testGeneralServiceNode.getMinMu(),
                    testGeneralServiceNode.getMinMuScv(),
                    serverCount);

//            LOG.info(testGeneralServiceNode.execServiceNodeList.get(maxI).toString());
            retVal += (maxAvgSojournTime * testGeneralServiceNode.getRatio());
        }
        return retVal;
    }

    public static double getExpectedTotalSojournTimeForGeneralizedOQN_SimpleApprBIA(Map<String, GeneralServiceNode> serviceNodes, Map<String, Integer> allocation) {

        double retVal = 0.0;
        for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();
            GeneralServiceNode serviceNode = e.getValue();
            int serverCount = allocation.get(cid).intValue();
            double avgSojournTime = sojournTime_GGK_SimpleAppr(
                    serviceNode.getLambdaByInterArrival(), serviceNode.getInterArrivalScv(), serviceNode.getMu(), serviceNode.getScvServTimeHis(), serverCount);
            retVal += (avgSojournTime * serviceNode.getRatioByInterArrival());
        }
        return retVal;
    }

    public static double getExpectedTotalSojournTimeForGeneralizedOQN_ComplexApprBIA(Map<String, GeneralServiceNode> serviceNodes, Map<String, Integer> allocation) {

        double retVal = 0.0;
        for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();
            GeneralServiceNode serviceNode = e.getValue();
            int serverCount = allocation.get(cid).intValue();
            double avgSojournTime = sojournTime_GGK_ComplexAppr(
                    serviceNode.getLambdaByInterArrival(), serviceNode.getInterArrivalScv(), serviceNode.getMu(), serviceNode.getScvServTimeHis(), serverCount);
            retVal += (avgSojournTime * serviceNode.getRatioByInterArrival());
        }
        return retVal;
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
     * @param serviceNodes
     * @param totalResourceCount
     * @return null if a) minReq of any component is Integer.MAX_VALUE (invalid parameter mu = 0.0)
     * b) total minReq can not be satisfied (total minReq > totalResourceCount)
     * otherwise, the Map data structure.
     */
    public static Map<String, Integer> suggestAllocationGeneralTopApplyMMK(Map<String, GeneralServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply M/M/K, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    GeneralServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated);
                    double afterAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getRatio();
//                    LOG.info(cid + "," + currentAllocated + ", lam: " + sn.getLambda() + ", mu: " + sn.getMu() + ",before: " + beforeAddT);
//                    LOG.info(cid + "," + currentAllocated + ", lam: " + sn.getLambda() + ", mu: " + sn.getMu() + ",after: " + afterAddT);
//                    LOG.info(cid + "," + currentAllocated + ", ratio: " + sn.getRatio() + ", diff: " + diff);
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        GeneralServiceNode sn = e.getValue();
                        int currentAllocated = retVal.get(cid);

                        double beforeAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated);
                        double afterAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }


    public static Map<String, Integer> suggestAllocationGeneralTopApplyMMK_exec(Map<String, GeneralServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), ((TestGeneralServiceNode)e.getValue()).getMinMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply M/M/K_Ex, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    TestGeneralServiceNode sn = (TestGeneralServiceNode) e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_MMK(sn.getLambda(), sn.getMinMu(), currentAllocated);
                    double afterAddT = sojournTime_MMK(sn.getLambda(), sn.getMinMu(), currentAllocated + 1);
                    double diff = (beforeAddT - afterAddT) * sn.getRatio();

//                    LOG.info(cid + "," + currentAllocated + ", lam: " + sn.getLambda() + ", mu: " + sn.getMinMu() + ",before: " + beforeAddT);
//                    LOG.info(cid + "," + currentAllocated + ", lam: " + sn.getLambda() + ", mu: " + sn.getMinMu() + ",after: " + afterAddT);
//                    LOG.info(cid + "," + currentAllocated + ", ratio: " + sn.getRatio() + ", diff: " + diff);
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        TestGeneralServiceNode sn = (TestGeneralServiceNode) e.getValue();
                        int currentAllocated = retVal.get(e.getKey());

                        double beforeAddT = sojournTime_MMK(sn.getLambda(), sn.getMinMu(), currentAllocated);
                        double afterAddT = sojournTime_MMK(sn.getLambda(), sn.getMinMu(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }

    public static Map<String, Integer> suggestAllocationGeneralTopApplyGGK_SimpleAppr(Map<String, GeneralServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply GGK_SimpleAppr, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    GeneralServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_GGK_SimpleAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                    double afterAddT = sojournTime_GGK_SimpleAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getRatio();
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        GeneralServiceNode sn = e.getValue();
                        int currentAllocated = retVal.get(cid);

                        double beforeAddT = sojournTime_GGK_SimpleAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                        double afterAddT = sojournTime_GGK_SimpleAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }


    public static Map<String, Integer> suggestAllocationGeneralTopApplyGGK_SimpleApprBIA(Map<String, GeneralServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply GGK_SimpleApprBIA, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    GeneralServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_GGK_SimpleAppr(sn.getLambdaByInterArrival(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                    double afterAddT = sojournTime_GGK_SimpleAppr(sn.getLambdaByInterArrival(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getRatioByInterArrival();
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        GeneralServiceNode sn = e.getValue();
                        int currentAllocated = retVal.get(cid);

                        double beforeAddT = sojournTime_GGK_SimpleAppr(sn.getLambdaByInterArrival(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                        double afterAddT = sojournTime_GGK_SimpleAppr(sn.getLambdaByInterArrival(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }

    public static Map<String, Integer> suggestAllocationGeneralTopApplyGGK_ComplexAppr(Map<String, GeneralServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply GGK_ComplexAppr, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    GeneralServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_GGK_ComplexAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                    double afterAddT = sojournTime_GGK_ComplexAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getRatio();
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        GeneralServiceNode sn = e.getValue();
                        int currentAllocated = retVal.get(cid);

                        double beforeAddT = sojournTime_GGK_ComplexAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                        double afterAddT = sojournTime_GGK_ComplexAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }

    public static Map<String, Integer> suggestAllocationGeneralTopApplyGGK_ComplexAppr_exec(Map<String, GeneralServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), ((TestGeneralServiceNode)e.getValue()).getMinMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply GGK_ComAppr_Ex, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    TestGeneralServiceNode sn = (TestGeneralServiceNode) e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_GGK_ComplexAppr(sn.getLambda(), sn.getInterArrivalScv(),
                            sn.getMinMu(), sn.getMinMuScv(), currentAllocated);

                    double afterAddT = sojournTime_GGK_ComplexAppr(sn.getLambda(), sn.getInterArrivalScv(),
                            sn.getMinMu(), sn.getMinMuScv(), currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getRatio();
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        TestGeneralServiceNode sn = (TestGeneralServiceNode) e.getValue();
                        int currentAllocated = retVal.get(e.getKey());

                        double beforeAddT = sojournTime_GGK_ComplexAppr(
                                sn.getLambda(), sn.getInterArrivalScv(),
                                sn.getMinMu(), sn.getMinMuScv(), currentAllocated);

                        double afterAddT = sojournTime_GGK_ComplexAppr(
                                sn.getLambda(), sn.getInterArrivalScv(),
                                sn.getMinMu(), sn.getMinMuScv(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }


    public static Map<String, Integer> suggestAllocationGeneralTopApplyGGK_ComplexApprBIA(Map<String, GeneralServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply GGK_ComplexApprBIA, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    GeneralServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_GGK_ComplexAppr(sn.getLambdaByInterArrival(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                    double afterAddT = sojournTime_GGK_ComplexAppr(sn.getLambdaByInterArrival(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getRatioByInterArrival();
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        GeneralServiceNode sn = e.getValue();
                        int currentAllocated = retVal.get(cid);

                        double beforeAddT = sojournTime_GGK_ComplexAppr(sn.getLambdaByInterArrival(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                        double afterAddT = sojournTime_GGK_ComplexAppr(sn.getLambdaByInterArrival(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }

    /**
     * Like Module A', input required QoS, output #threads required
     * Here we separate to two modules: first output allocation, then calculate total #threads included.
     * Caution all the computation involved is in second unit.
     *
     * @param serviceNodes
     * @param maxAllowedCompleteTimeSeconds, the unit here is second! consistent with function getErlangChainTopCompleteTime()
     * @param lowerBoundDelta,               this is to set the offset of the lowerBoundServiceTime, we require delta to be positive, and 0 as default.
     * @param adjRatio,                      this is to adjust the estimated ErlangServiceTime to fit more closely to the real measured complte time
     * @return null if a) any service node is not in the valid state (mu = 0.0), this is not the case of rho > 1.0, just for checking mu
     * b) lowerBoundServiceTime > requiredQoS
     */
    public static Map<String, Integer> getMinReqServerAllocationGeneralTopApplyMMK(Map<String, GeneralServiceNode> serviceNodes,
                                                                                   double maxAllowedCompleteTimeSeconds,
                                                                                   double lowerBoundDelta,
                                                                                   double adjRatio,
                                                                                   int maxAvailableExec) {
        double lowerBoundServiceTime = 0.0;
        int totalMinReq = 0;
        for (Map.Entry<String, GeneralServiceNode> e : serviceNodes.entrySet()) {
            double lambda = e.getValue().getLambda();
            double mu = e.getValue().getMu();
            ///caution, the unit should be millisecond
            lowerBoundServiceTime += (1.0 / mu);
            totalMinReq += getMinReqServerCount(lambda, mu);
        }

        Map<String, Integer> currAllocation = null;
        if (lowerBoundServiceTime * adjRatio + lowerBoundDelta < maxAllowedCompleteTimeSeconds) {
            double currTime;
            do {
                currAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, totalMinReq);
                currTime = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, currAllocation) * adjRatio;

                LOG.debug(String.format("getMinReqServAllcQoS(ms): %.4f, rawCompleteTime(ms): %.4f, afterAdjust(ms): %.4f, totalMinReqQoS: %d",
                        maxAllowedCompleteTimeSeconds * 1000.0, currTime * 1000.0 / adjRatio, currTime * 1000.0, totalMinReq));

                totalMinReq++;
                //check: we need to check totalMinReq to avoid infinite loop!
            } while (currTime > maxAllowedCompleteTimeSeconds && totalMinReq <= maxAvailableExec);
        }
        return totalMinReq <= maxAvailableExec ? currAllocation : null;
    }

    public static Map<String, Integer> getMinReqServerAllocationGeneralTopApplyMMK(Map<String, GeneralServiceNode> serviceNodes,
                                                                                   double maxAllowedCompleteTime,
                                                                                   double adjRatio,
                                                                                   int maxAvailableExec) {
        return getMinReqServerAllocationGeneralTopApplyMMK(serviceNodes, maxAllowedCompleteTime, 0.0, adjRatio, maxAvailableExec);
    }

    public static int totalServerCountInvolved(Map<String, Integer> allocation) {
        return Objects.requireNonNull(allocation).values().stream().mapToInt(i -> i).sum();
    }


    /**
     * @param queueingNetwork
     * @param queueingNetwork
     * @param targetQoSMilliSec
     * @param currBoltAllocation
     * @param maxAvailable4Bolt
     * @param currentUsedThreadByBolts
     * @return status indicates whether the demanded QoS can be achieved or not
     * minReqAllocaiton, the minimum required resource (under optimized allocation) which can satisfy QoS
     * after: the optimized allocation under given maxAvailable4Bolt
     */
    public static AllocResult checkOptimized_MMK(GeneralSourceNode sourceNode, Map<String, GeneralServiceNode> queueingNetwork,
                                                 double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                 int maxAvailable4Bolt, int currentUsedThreadByBolts) {

        ///Todo we need to do the stability check first here!
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        double estTotalSojournTimeMilliSec_MMK = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currBoltAllocation);
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_MMK) * 100.0 / realLatencyMilliSeconds;

        LOG.info(String.format("realLatency(ms): %.4f, estMMK: %.4f, urMMK: %.4f, reMMK: %.4f",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, underEstimateRatio, relativeError));

        LOG.debug("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTopApplyMMK(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.debug("Find out best allocation given available executors.");
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, currentUsedThreadByBolts);
        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estMMK", estTotalSojournTimeMilliSec_MMK);
        context.put("urMMK", underEstimateRatio);
        context.put("reMMK", relativeError);
        return new AllocResult(status, minReqAllocation, currOptAllocation, kMaxOptAllocation).setContext(context);
    }

    /**
     * @param queueingNetwork
     * @param queueingNetwork
     * @param targetQoSMilliSec
     * @param currBoltAllocation
     * @param maxAvailable4Bolt
     * @param currentUsedThreadByBolts
     * @return status indicates whether the demanded QoS can be achieved or not
     * minReqAllocaiton, the minimum required resource (under optimized allocation) which can satisfy QoS
     * after: the optimized allocation under given maxAvailable4Bolt
     */
    public static AllocResult checkOptimized_MMK_exec(GeneralSourceNode sourceNode, Map<String, GeneralServiceNode> queueingNetwork,
                                                      double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                      int maxAvailable4Bolt, int currentUsedThreadByBolts) {

        ///Todo we need to do the stability check first here!
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        double estTotalSojournTimeMilliSec_MMK = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN_exec(queueingNetwork, currBoltAllocation);
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_MMK) * 100.0 / realLatencyMilliSeconds;

        LOG.info(String.format("realLatency(ms): %.4f, estMMKex: %.4f, urMMKex: %.4f, reMMKex: %.4f",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, underEstimateRatio, relativeError));

        LOG.debug("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTopApplyMMK(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.debug("Find out best allocation given available executors.");
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyMMK_exec(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyMMK_exec(queueingNetwork, currentUsedThreadByBolts);
        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estMMKex", estTotalSojournTimeMilliSec_MMK);
        context.put("urMMKex", underEstimateRatio);
        context.put("reMMKex", relativeError);
        return new AllocResult(status, minReqAllocation, currOptAllocation, kMaxOptAllocation).setContext(context);
    }

    /**
     * @param queueingNetwork
     * @param queueingNetwork
     * @param targetQoSMilliSec
     * @param currBoltAllocation
     * @param maxAvailable4Bolt
     * @param currentUsedThreadByBolts
     * @return status indicates whether the demanded QoS can be achieved or not
     * minReqAllocaiton, the minimum required resource (under optimized allocation) which can satisfy QoS
     * after: the optimized allocation under given maxAvailable4Bolt
     */
    public static AllocResult checkOptimized_MMK_execAvg(GeneralSourceNode sourceNode, Map<String, GeneralServiceNode> queueingNetwork,
                                                       double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                       int maxAvailable4Bolt, int currentUsedThreadByBolts) {

        ///Todo we need to do the stability check first here!
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        double estTotalSojournTimeMilliSec_MMK = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN_execAvg(queueingNetwork, currBoltAllocation);
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_MMK) * 100.0 / realLatencyMilliSeconds;

        LOG.info(String.format("realLatency(ms): %.4f, estMMKexA: %.4f, urMMKexA: %.4f, reMMKexA: %.4f",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, underEstimateRatio, relativeError));

        LOG.debug("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTopApplyMMK(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.debug("Find out best allocation given available executors.");
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, currentUsedThreadByBolts);
        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estMMKexA", estTotalSojournTimeMilliSec_MMK);
        context.put("urMMKexA", underEstimateRatio);
        context.put("reMMKexA", relativeError);
        return new AllocResult(status, minReqAllocation, currOptAllocation, kMaxOptAllocation).setContext(context);
    }

    public static AllocResult checkOptimized_GGK_SimpleAppr(GeneralSourceNode sourceNode, Map<String, GeneralServiceNode> queueingNetwork,
                                                            double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                            int maxAvailable4Bolt, int currentUsedThreadByBolts) {

        ///Todo we need to do the stability check first here!
        ///TODO so far, we still use getMinReqServerAllocationGeneralTopApplyMMK to get minReqValue for temp use
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        double estTotalSojournTimeMilliSec_GGK_SAppr = 1000.0 * getExpectedTotalSojournTimeForGeneralizedOQN_SimpleAppr(queueingNetwork, currBoltAllocation);
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_GGK_SAppr);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_GGK_SAppr) * 100.0 / realLatencyMilliSeconds;

        LOG.info(String.format("realLatency(ms): %.4f, estGGKSAppr: %.4f, urGGKSAppr: %.4f, reGGKSAppr: %.4f",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_GGK_SAppr, underEstimateRatio, relativeError));

        LOG.debug("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTopApplyMMK(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.debug("Find out best allocation given available executors.");
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyGGK_SimpleAppr(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyGGK_SimpleAppr(queueingNetwork, currentUsedThreadByBolts);
        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estGGKSAppr", estTotalSojournTimeMilliSec_GGK_SAppr);
        context.put("urGGKSAppr", underEstimateRatio);
        context.put("reGGKSAppr", relativeError);
        return new AllocResult(status, minReqAllocation, currOptAllocation, kMaxOptAllocation).setContext(context);
    }

    public static AllocResult checkOptimized_GGK_SimpleApprBIA(GeneralSourceNode sourceNode, Map<String, GeneralServiceNode> queueingNetwork,
                                                               double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                               int maxAvailable4Bolt, int currentUsedThreadByBolts) {

        ///Todo we need to do the stability check first here!
        ///TODO so far, we still use getMinReqServerAllocationGeneralTopApplyMMK to get minReqValue for temp use
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        double estTotalSojournTimeMilliSec_GGK_SApprBIA = 1000.0 * getExpectedTotalSojournTimeForGeneralizedOQN_SimpleApprBIA(queueingNetwork, currBoltAllocation);
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_GGK_SApprBIA);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_GGK_SApprBIA) * 100.0 / realLatencyMilliSeconds;

        LOG.info(String.format("realLatency(ms): %.4f, estGGKSApprBIA: %.4f, urGGKSApprBIA: %.4f, reGGKSApprBIA: %.4f",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_GGK_SApprBIA, underEstimateRatio, relativeError));

        LOG.debug("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTopApplyMMK(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.debug("Find out best allocation given available executors.");
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyGGK_SimpleApprBIA(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyGGK_SimpleApprBIA(queueingNetwork, currentUsedThreadByBolts);
        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estGGKSApprBIA", estTotalSojournTimeMilliSec_GGK_SApprBIA);
        context.put("urGGKSApprBIA", underEstimateRatio);
        context.put("reGGKSApprBIA", relativeError);
        return new AllocResult(status, minReqAllocation, currOptAllocation, kMaxOptAllocation).setContext(context);
    }

    public static AllocResult checkOptimized_GGK_ComplexAppr(GeneralSourceNode sourceNode, Map<String, GeneralServiceNode> queueingNetwork,
                                                             double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                             int maxAvailable4Bolt, int currentUsedThreadByBolts) {

        ///Todo we need to do the stability check first here!
        ///TODO so far, we still use getMinReqServerAllocationGeneralTopApplyMMK to get minReqValue for temp use
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        double estTotalSojournTimeMilliSec_GGK_CAppr = 1000.0 * getExpectedTotalSojournTimeForGeneralizedOQN_ComplexAppr(queueingNetwork, currBoltAllocation);
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_GGK_CAppr);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_GGK_CAppr) * 100.0 / realLatencyMilliSeconds;

        LOG.info(String.format("realLatency(ms): %.4f, estGGKCAppr: %.4f, urGGKCAppr: %.4f, reGGKCAppr: %.4f",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_GGK_CAppr, underEstimateRatio, relativeError));

        LOG.debug("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTopApplyMMK(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.debug("Find out best allocation given available executors.");
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyGGK_ComplexAppr(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyGGK_ComplexAppr(queueingNetwork, currentUsedThreadByBolts);
        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estGGKCAppr", estTotalSojournTimeMilliSec_GGK_CAppr);
        context.put("urGGKCAppr", underEstimateRatio);
        context.put("reGGKCAppr", relativeError);
        return new AllocResult(status, minReqAllocation, currOptAllocation, kMaxOptAllocation).setContext(context);
    }

    public static AllocResult checkOptimized_GGK_ComplexAppr_exec(GeneralSourceNode sourceNode, Map<String, GeneralServiceNode> queueingNetwork,
                                                                  double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                                  int maxAvailable4Bolt, int currentUsedThreadByBolts) {

        ///Todo we need to do the stability check first here!
        ///TODO so far, we still use getMinReqServerAllocationGeneralTopApplyMMK to get minReqValue for temp use
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        double estTotalSojournTimeMilliSec_GGK_CAppr = 1000.0 * getExpectedTotalSojournTimeForGeneralizedOQN_ComplexAppr_exec(queueingNetwork, currBoltAllocation);
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_GGK_CAppr);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_GGK_CAppr) * 100.0 / realLatencyMilliSeconds;

        LOG.info(String.format("realLatency(ms): %.4f, estGGKCApprEx: %.4f, urGGKCApprEx: %.4f, reGGKCApprEx: %.4f",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_GGK_CAppr, underEstimateRatio, relativeError));

        LOG.debug("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTopApplyMMK(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.debug("Find out best allocation given available executors.");
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyGGK_ComplexAppr_exec(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyGGK_ComplexAppr_exec(queueingNetwork, currentUsedThreadByBolts);
        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estGGKCApprEx", estTotalSojournTimeMilliSec_GGK_CAppr);
        context.put("urGGKCApprEx", underEstimateRatio);
        context.put("reGGKCApprEx", relativeError);
        return new AllocResult(status, minReqAllocation, currOptAllocation, kMaxOptAllocation).setContext(context);
    }

    public static AllocResult checkOptimized_GGK_ComplexApprBIA(GeneralSourceNode sourceNode, Map<String, GeneralServiceNode> queueingNetwork,
                                                                double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                                int maxAvailable4Bolt, int currentUsedThreadByBolts) {

        ///Todo we need to do the stability check first here!
        ///TODO so far, we still use getMinReqServerAllocationGeneralTopApplyMMK to get minReqValue for temp use
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        double estTotalSojournTimeMilliSec_GGK_CApprBIA = 1000.0 * getExpectedTotalSojournTimeForGeneralizedOQN_ComplexApprBIA(queueingNetwork, currBoltAllocation);
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_GGK_CApprBIA);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_GGK_CApprBIA) * 100.0 / realLatencyMilliSeconds;

        LOG.info(String.format("realLatency(ms): %.4f, estGGKCApprBIA: %.4f, urGGKCApprBIA: %.4f, reGGKCApprBIA: %.4f",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_GGK_CApprBIA, underEstimateRatio, relativeError));

        LOG.debug("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTopApplyMMK(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.debug("Find out best allocation given available executors.");
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyGGK_ComplexApprBIA(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyGGK_ComplexApprBIA(queueingNetwork, currentUsedThreadByBolts);
        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estGGKCApprBIA", estTotalSojournTimeMilliSec_GGK_CApprBIA);
        context.put("urGGKCApprBIA", underEstimateRatio);
        context.put("reGGKCApprBIA", relativeError);
        return new AllocResult(status, minReqAllocation, currOptAllocation, kMaxOptAllocation).setContext(context);
    }

    /// The adjustment on Var(x) has been tested. The effect is too small (since n is quite large), therefore, we decide to ignore this.
    public static AllocResult checkOptimized(GeneralSourceNode sourceNode, Map<String, GeneralServiceNode> queueingNetwork,
                                             double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                             int maxAvailable4Bolt, int currentUsedThreadByBolts, ServiceModelType retAlloType) {

        AllocResult allocResult[] = new AllocResult[ServiceModelType.totalTypeCount];

        allocResult[0] = checkOptimized_MMK(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
        LOG.info("MMK,  minReqAllo: " + allocResult[0].minReqOptAllocation + ", minReqStatus: " + allocResult[0].status);
        LOG.info("MMK, currOptAllo: " + allocResult[0].currOptAllocation);
        LOG.info("MMK, kMaxOptAllo: " + allocResult[0].kMaxOptAllocation);

        allocResult[1] = checkOptimized_MMK_exec(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
        LOG.info("MMKEx,  minReqAllo: " + allocResult[1].minReqOptAllocation + ", minReqStatus: " + allocResult[1].status);
        LOG.info("MMKEx, currOptAllo: " + allocResult[1].currOptAllocation);
        LOG.info("MMKEx, kMaxOptAllo: " + allocResult[1].kMaxOptAllocation);

        allocResult[4] = checkOptimized_MMK_execAvg(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
//        LOG.info("MMKex,  minReqAllo: " + allocResult[1].minReqOptAllocation + ", minReqStatus: " + allocResult[1].status);
//        LOG.info("MMKex, currOptAllo: " + allocResult[1].currOptAllocation);
//        LOG.info("MMKex, kMaxOptAllo: " + allocResult[1].kMaxOptAllocation);
//        allocResult[1] = checkOptimized_GGK_SimpleAppr(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
//        LOG.info("GGKSAppr,  minReqAllo: " + allocResult[1].minReqOptAllocation + ", minReqStatus: " + allocResult[1].status);
//        LOG.info("GGKSAppr, currOptAllo: " + allocResult[1].currOptAllocation);
//        LOG.info("GGKSAppr, kMaxOptAllo: " + allocResult[1].kMaxOptAllocation);
        allocResult[2] = checkOptimized_GGK_ComplexAppr(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
        LOG.info("GGKCAppr,  minReqAllo: " + allocResult[2].minReqOptAllocation + ", minReqStatus: " + allocResult[2].status);
        LOG.info("GGKCAppr, currOptAllo: " + allocResult[2].currOptAllocation);
        LOG.info("GGKCAppr, kMaxOptAllo: " + allocResult[2].kMaxOptAllocation);

        allocResult[3] = checkOptimized_GGK_ComplexAppr_exec(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
        LOG.info("GGKCApprEx,  minReqAllo: " + allocResult[3].minReqOptAllocation + ", minReqStatus: " + allocResult[3].status);
        LOG.info("GGKCApprEx, currOptAllo: " + allocResult[3].currOptAllocation);
        LOG.info("GGKCApprEx, kMaxOptAllo: " + allocResult[3].kMaxOptAllocation);
////        allocResult[2] = checkOptimized_GGK_SimpleApprBIA(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
////        LOG.info("GGKSApprBIA,  minReqAllo: " + allocResult[2].minReqOptAllocation + ", minReqStatus: " + allocResult[2].status);
////        LOG.info("GGKSApprBIA, currOptAllo: " + allocResult[2].currOptAllocation);
////        LOG.info("GGKSApprBIA, kMaxOptAllo: " + allocResult[2].kMaxOptAllocation);
////        allocResult[4] = checkOptimized_GGK_ComplexApprBIA(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
////        LOG.info("GGKCApprBIA,  minReqAllo: " + allocResult[4].minReqOptAllocation + ", minReqStatus: " + allocResult[4].status);
////        LOG.info("GGKCApprBIA, currOptAllo: " + allocResult[4].currOptAllocation);
////        LOG.info("GGKCApprBIA, kMaxOptAllo: " + allocResult[4].kMaxOptAllocation);

        Map<String, Object> context = new HashMap<>();
//        for (int i = 0; i < allocResult.length; i ++){
//                context.putAll((Map<String, Object>)allocResult[i].getContext());
//        }
        context.putAll((Map<String, Object>) allocResult[0].getContext());
        context.putAll((Map<String, Object>) allocResult[1].getContext());
        context.putAll((Map<String, Object>) allocResult[2].getContext());
        context.putAll((Map<String, Object>) allocResult[3].getContext());


//        allocResult[2] = checkOptimized_GGK_SimpleApprAdj(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
//        LOG.info("GGKSApprAdj,  minReqAllo: " + allocResult[2].minReqOptAllocation + ", minReqStatus: " + allocResult[2].status);
//        LOG.info("GGKSApprAdj, currOptAllo: " + allocResult[2].currOptAllocation);
//        LOG.info("GGKSApprAdj, kMaxOptAllo: " + allocResult[2].kMaxOptAllocation);

        return allocResult[retAlloType.getValue()].setContext(context);
    }

    /**
     * if mu = 0.0 or serverCount not positive, then rho is not defined, we consider it as the unstable case (represented by Double.MAX_VALUE)
     * otherwise, return the calculation results. Leave the interpretation to the calling function, like isStable();
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    private static double getRho(double lambda, double mu, int serverCount) {
        return (mu > 0.0 && serverCount > 0) ? lambda / (mu * (double) serverCount) : Double.MAX_VALUE;
    }

    /**
     * First call getRho,
     * then determine when rho is validate, i.e., rho < 1.0
     * otherwise return unstable (FALSE)
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    public static boolean isStable(double lambda, double mu, int serverCount) {
        return getRho(lambda, mu, serverCount) < 1.0;
    }

    private static double getRhoSingleServer(double lambda, double mu) {
        return getRho(lambda, mu, 1);
    }

    public static int getMinReqServerCount(double lambda, double mu) {
        return (int) (lambda / mu) + 1;
    }


    /**
     * we assume the stability check is done before calling this function
     * The total sojournTime of an MMK queue is the sum of queueing time and expected service time (1.0 / mu).
     *
     * @param lambda,     average arrival rate
     * @param mu,         average execute rate
     * @param serverCount
     * @return
     */
    public static double sojournTime_MMK(double lambda, double mu, int serverCount) {
        return avgQueueingTime_MMK(lambda, mu, serverCount) + 1.0 / mu;
    }

    /**
     * we assume the stability check is done before calling this function
     * This is a simple version of approximation on average sojourn time for the G/G/K queue
     * E(Wq) = (caj+csj)/2 E(Wq(M/M/K)) --> Eq. 62 of survey paper on OQN by Gabriel Bitran
     *
     * @param lambda
     * @param scvArrival, scv of inter-arrival times
     * @param mu
     * @param scvService, scv of service times
     * @param serverCount
     * @return
     */
    public static double sojournTime_GGK_SimpleAppr(double lambda, double scvArrival, double mu, double scvService, int serverCount) {
        double adjust = (scvArrival + scvService) / 2.0;
        return avgQueueingTime_MMK(lambda, mu, serverCount) * adjust + 1.0 / mu;
    }

    /**
     * we assume the stability check is done before calling this function
     * This is a more complex and accurate version of approximation on average sojourn time for the G/G/K queue
     * E(Wq) = \Phi(rho, caj, csj, k) * (caj+csj)/2 E(Wq(M/M/K)) --> Appendix 1 of "Machine allocation algorithms for job shop manufacturing", van Vliet and R. Kan
     *
     * @param lambda
     * @param scvArrival, scv of inter-arrival times
     * @param mu
     * @param scvService, scv of service times
     * @param serverCount
     * @return
     */
    public static double sojournTime_GGK_ComplexAppr(double lambda, double scvArrival, double mu, double scvService, int serverCount) {

        double k = serverCount;
        double rho = lambda / (mu * k);
        double gamma = (1.0 - rho) * (k - 1.0) * (Math.sqrt(4.0 + 5.0 * k) - 2.0) / (16.0 * k * rho);
        gamma = Math.min(0.24, gamma);

        double f1 = 1.0 + gamma;
        double f2 = 1.0 - 4.0 * gamma;
        double f3 = f2 * Math.exp(-2.0 * (1.0 - rho) / (3.0 * rho));
        double f4 = Math.min(1.0, (f1 + f3) / 2.0);

        double adjust = (scvArrival + scvService) / 2.0;
        double Psi = adjust < 1.0 ? Math.pow(f4, 2.0 - 2.0 * adjust) : 1.0;

//        double Phy = 0.0;
//        if (scvArrival >= scvService) {
//            Phy = f1 * (scvArrival - scvService) / (scvArrival - 0.75 * scvService)
//                    + Psi * scvService / (4.0 * scvArrival - 3.0 * scvService);
//        } else {
//            Phy = f3 * 0.5 * (scvService - scvArrival) / (scvArrival + scvService)
//                    + Psi * 0.5 * (scvService + 3.0 * scvArrival) / (scvArrival + scvService);
//        }
//      simplified as:
        double Phy = (scvArrival >= scvService)
                ? (f1 * (scvArrival - scvService) / (scvArrival - 0.75 * scvService)
                + Psi * scvService / (4.0 * scvArrival - 3.0 * scvService))
                : (f3 * 0.5 * (scvService - scvArrival) / (scvArrival + scvService)
                + Psi * 0.5 * (scvService + 3.0 * scvArrival) / (scvArrival + scvService));

        return avgQueueingTime_MMK(lambda, mu, serverCount) * adjust * Phy + 1.0 / mu;
    }

    /**
     * we assume the stability check is done before calling this function
     * This is a standard erlang-C formula
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    public static double avgQueueingTime_MMK(double lambda, double mu, int serverCount) {
        double r = lambda / (mu * (double) serverCount);
        double kr = lambda / mu;

        double phi0_p1 = 1.0;
        for (int i = 1; i < serverCount; i++) {
            double a = Math.pow(kr, i);
            double b = (double) factorial(i);
            phi0_p1 += (a / b);
        }

        double phi0_p2_nor = Math.pow(kr, serverCount);
        double phi0_p2_denor = (1.0 - r) * (double) (factorial(serverCount));
        double phi0_p2 = phi0_p2_nor / phi0_p2_denor;

        double phi0 = 1.0 / (phi0_p1 + phi0_p2);

        double pWait = phi0_p2 * phi0;

        double waitingTime = pWait * r / ((1.0 - r) * lambda);

        return waitingTime;
    }


    public static double sojournTime_MM1(double lambda, double mu) {
        return 1.0 / (mu - lambda);
    }

    private static int factorial(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("Attention, negative input is not allowed: " + n);
        } else if (n == 0) {
            return 1;
        } else {
            int ret = 1;
            for (int i = 2; i <= n; i++) {
                ret = ret * i;
            }
            return ret;
        }
    }
}
