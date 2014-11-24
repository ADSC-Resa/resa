package resa.scheduler;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.metrics.MeasuredData;
import resa.optimize.*;
import resa.util.ConfigUtil;
import resa.util.ResaUtils;
import resa.util.TopologyHelper;

import java.util.*;
import java.util.stream.Collectors;

import static resa.util.ResaConfig.*;

/**
 * Created by ding on 14-4-26.
 */
public class TopologyOptimizer {

    public static interface MeasuredSource {
        public Iterable<MeasuredData> retrieve();
    }

    private static final Logger LOG = LoggerFactory.getLogger(TopologyOptimizer.class);

    private final Timer timer = new Timer(true);
    private Map<String, Integer> currAllocation;
    private int maxExecutorsPerWorker;
    private int topologyMaxExecutors;
    private int rebalanceWaitingSecs;
    private Nimbus.Client nimbus;
    private String topologyName;
    private String topologyId;
    private StormTopology rawTopology;
    private Map<String, Object> conf;
    private MeasuredSource measuredSource;
    private AllocCalculator allocCalculator;
    private DecisionMaker decisionMaker;

    public void init(String topologyName, Map<String, Object> conf, MeasuredSource measuredSource) {
        this.conf = conf;
        this.topologyName = topologyName;
        this.measuredSource = measuredSource;
        maxExecutorsPerWorker = ConfigUtil.getInt(conf, MAX_EXECUTORS_PER_WORKER, 8);
        topologyMaxExecutors = ConfigUtil.getInt(conf, ALLOWED_EXECUTOR_NUM, -1);
        rebalanceWaitingSecs = ConfigUtil.getInt(conf, REBALANCE_WAITING_SECS, -1);
        // connected to nimbus
        nimbus = NimbusClient.getConfiguredClient(conf).getClient();
        this.topologyId = TopologyHelper.getTopologyId(nimbus, topologyName);
        this.rawTopology = getRawTopologyThrow();
        // create Allocation Calculator
        allocCalculator = ResaUtils.newInstanceThrow((String) conf.getOrDefault(ALLOC_CALC_CLASS,
                SimpleGeneralAllocCalculator.class.getName()), AllocCalculator.class);
        // current allocation should be retrieved from nimbus
        currAllocation = getTopologyCurrAllocation();
        allocCalculator.init(conf, Collections.unmodifiableMap(currAllocation), rawTopology);
        // create Decision Maker
        decisionMaker = ResaUtils.newInstanceThrow((String) conf.getOrDefault(DECISION_MAKER_CLASS,
                DefaultDecisionMaker.class.getName()), DecisionMaker.class);
        decisionMaker.init(conf, rawTopology);
        LOG.info("AllocCalculator class:" + allocCalculator.getClass().getName());
        LOG.info("DecisionMaker class:" + decisionMaker.getClass().getName());
    }

    public void start() {
        long calcInterval = ConfigUtil.getInt(conf, OPTIMIZE_INTERVAL, 30) * 1000;
        //start optimize thread
        timer.scheduleAtFixedRate(new OptimizeTask(), calcInterval * 2, calcInterval);
        LOG.info(String.format("Init Topology Optimizer successfully with calc interval is %dms", calcInterval));
    }

    public void stop() {
        timer.cancel();
    }

    private class OptimizeTask extends TimerTask {

        @Override
        public void run() {
            Iterable<MeasuredData> data = measuredSource.retrieve();
            // get current ExecutorDetails from nimbus
            Map<String, List<ExecutorDetails>> topoExecutors = TopologyHelper.getTopologyExecutors(nimbus, topologyId)
                    .entrySet().stream().filter(e -> !Utils.isSystemId(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            // TODO: Executors == null means nimbus temporarily unreachable or this topology has been killed
            Map<String, Integer> allc = topoExecutors != null ? calcAllocation(topoExecutors) : null;
            if (allc != null && !allc.equals(currAllocation)) {
                LOG.info("Topology allocation changed");
                currAllocation = allc;
                // discard old MeasuredData
                consumeData(data);
                allocCalculator.allocationChanged(Collections.unmodifiableMap(currAllocation));
            } else {
                AggResultCalculator calculator = new AggResultCalculator(data, topoExecutors, rawTopology);
                calculator.calCMVStat();
                //TODO: (added by Tom) we need to calc the maxProcessedDataSize as a configuration parameter.
                // set a return value (count) from calculator.calCMVStat()
                // if the count == maxProcessedDataSize (current is 500, say), we need to do something,
                // since otherwise, the measurement data is too obsolete
                Map<String, Integer> newAllocation = calcNewAllocation(calculator.getResults());
                if (newAllocation != null && !newAllocation.equals(currAllocation)) {
                    LOG.info("Detected topology allocation changed, request rebalance....");
                    LOG.info("Old allc is " + currAllocation);
                    LOG.info("new allc is " + newAllocation);
                    requestRebalance(newAllocation);
                }
            }
        }

        private void consumeData(Iterable<MeasuredData> data) {
            data.forEach((e) -> {
            });
        }

        private Map<String, Integer> calcAllocation(Map<String, List<ExecutorDetails>> topoExecutors) {
            return topoExecutors.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
        }

        private Map<String, Integer> calcNewAllocation(Map<String, AggResult[]> data) {
            int maxExecutors = topologyMaxExecutors == -1 ? Math.max(ConfigUtil.getInt(conf, Config.TOPOLOGY_WORKERS, 1),
                    getNumWorkers(currAllocation)) * maxExecutorsPerWorker : topologyMaxExecutors;
            Map<String, Integer> ret = null;
            try {
                AllocResult decision = allocCalculator.calc(data, maxExecutors);
                // tagged by Tom, modified by troy:
                // in decisionMaker , we need to improve this rebalance step to calc more stable and smooth
                // Idea 1) we can maintain an decision list, only when we have received continuous
                // decision with x times (x is the parameter), we will do rebalance (so that unstable oscillation
                // is removed)
                // Idea 2) we need to consider the expected gain (by consider the expected QoS gain) as a weight,
                // which should be contained in the AllocResult object.
                ret = decisionMaker.make(decision, Collections.unmodifiableMap(currAllocation));
            } catch (Throwable e) {
                LOG.warn("calc new allocation failed", e);
            }
            return ret;
        }
    }

    private StormTopology getRawTopologyThrow() {
        try {
            return nimbus.getUserTopology(topologyId);
        } catch (Exception e) {
            throw new RuntimeException("Get raw topology failed, id is " + topologyId, e);
        }
    }

    /* call nimbus to get current topology allocation */
    private Map<String, Integer> getTopologyCurrAllocation() {
        try {
            TopologyInfo topoInfo = nimbus.getTopologyInfo(topologyId);
            return topoInfo.get_executors().stream().filter(e -> !Utils.isSystemId(e.get_component_id()))
                    .collect(Collectors.groupingBy(e -> e.get_component_id(),
                            Collectors.reducing(0, e -> 1, (i1, i2) -> i1 + i2)));
        } catch (Exception e) {
            LOG.warn("Get topology curr allocation from nimbus failed", e);
        }
        return Collections.emptyMap();
    }

    private int getNumWorkers(Map<String, Integer> allocation) {
        int totolNumExecutors = allocation.values().stream().mapToInt(Integer::intValue).sum();
        int numWorkers = totolNumExecutors / maxExecutorsPerWorker;
        if (totolNumExecutors % maxExecutorsPerWorker > (int) (maxExecutorsPerWorker / 2)) {
            numWorkers++;
        }
        return numWorkers;
    }

    /* Send rebalance request to nimbus */
    private boolean requestRebalance(Map<String, Integer> allocation) {
        int numWorkers = getNumWorkers(allocation);
        RebalanceOptions options = new RebalanceOptions();
        //set rebalance options
        options.set_num_workers(numWorkers);
        options.set_num_executors(allocation);
        if (rebalanceWaitingSecs >= 0) {
            options.set_wait_secs(rebalanceWaitingSecs);
        }
        try {
            nimbus.rebalance(topologyName, options);
            LOG.info("do rebalance successfully for topology " + topologyName);
            return true;
        } catch (Exception e) {
            LOG.warn("do rebalance failed for topology " + topologyName, e);
        }
        return false;
    }

}
