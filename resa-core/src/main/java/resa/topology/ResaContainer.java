package resa.topology;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.metrics.FilteredMetricsCollector;
import resa.metrics.MeasuredData;
import resa.metrics.MetricNames;
import resa.util.ConfigUtil;
import resa.util.TopologyHelper;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static resa.util.ResaConfig.REBALANCE_WAITING_SECS;

/**
 * Created by ding on 14-5-5.
 */
public class ResaContainer extends FilteredMetricsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(ResaContainer.class);

    private TopologyOptimizer topologyOptimizer = new TopologyOptimizer();
    private ContainerContext ctx;
    private Nimbus.Client nimbus;
    private String topologyName;
    private String topologyId;
    private Map<String, Object> conf;

    @Override
    public void prepare(Map conf, Object arg, TopologyContext context, IErrorReporter errorReporter) {
        super.prepare(conf, arg, context, errorReporter);
        this.conf = conf;
        this.topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        // connected to nimbus
        nimbus = NimbusClient.getConfiguredClient(conf).getClient();
        topologyId = TopologyHelper.getTopologyId(nimbus, topologyName);
        // add approved metric name
        addApprovedMetirc("__sendqueue", MetricNames.SEND_QUEUE);
        addApprovedMetirc("__receive", MetricNames.RECV_QUEUE);
        addApprovedMetirc(MetricNames.COMPLETE_LATENCY);
        addApprovedMetirc(MetricNames.TASK_EXECUTE);
        addApprovedMetirc(MetricNames.EMIT_COUNT);
        addApprovedMetirc(MetricNames.DURATION);

        ctx = new ContainerContextImpl(context.getRawTopology(), conf);
        // topology optimizer will start its own thread
        // if more services required to start, maybe we need to extract a new interface here
        topologyOptimizer.init(ctx);
        topologyOptimizer.start();
    }

    private class ContainerContextImpl extends ContainerContext {

        protected ContainerContextImpl(StormTopology topology, Map<String, Object> conf) {
            super(topology, conf);
        }

        @Override
        public void emitMetric(String key, Object data) {

        }

        @Override
        public Map<String, List<ExecutorDetails>> runningExecutors() {
            return TopologyHelper.getTopologyExecutors(nimbus, topologyId).entrySet().stream()
                    .filter(e -> !Utils.isSystemId(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public boolean requestRebalance(Map<String, Integer> allocation, int numWorkers) {
            RebalanceOptions options = new RebalanceOptions();
            //set rebalance options
            options.set_num_workers(numWorkers);
            options.set_num_executors(allocation);
            int waitingSecs = ConfigUtil.getInt(conf, REBALANCE_WAITING_SECS, -1);
            if (waitingSecs >= 0) {
                options.set_wait_secs(waitingSecs);
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

    @Override
    protected void handleSelectedDataPoints(IMetricsConsumer.TaskInfo taskInfo,
                                            Collection<IMetricsConsumer.DataPoint> dataPoints) {
        Map<String, Object> ret = dataPoints.stream().collect(Collectors.toMap(p -> p.name, p -> p.value));
        MeasuredData measuredData = new MeasuredData(taskInfo.srcComponentId, taskInfo.srcTaskId,
                taskInfo.timestamp, ret);
        ctx.getListeners().forEach(l -> l.measuredDataReceived(measuredData));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        topologyOptimizer.stop();
    }
}
