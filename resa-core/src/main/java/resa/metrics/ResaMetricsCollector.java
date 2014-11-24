package resa.metrics;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import resa.scheduler.TopologyOptimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-5-5.
 */
public class ResaMetricsCollector extends FilteredMetricsCollector {

    private int bufferSize = 100;
    private volatile List<MeasuredData> measureBuffer;
    private TopologyOptimizer topologyOptimizer = new TopologyOptimizer();

    @Override
    public void prepare(Map conf, Object arg, TopologyContext context, IErrorReporter errorReporter) {
        super.prepare(conf, arg, context, errorReporter);
        // add approved metric name
        addApprovedMetirc("__sendqueue", MetricNames.SEND_QUEUE);
        addApprovedMetirc("__receive", MetricNames.RECV_QUEUE);
        addApprovedMetirc(MetricNames.COMPLETE_LATENCY);
        addApprovedMetirc(MetricNames.TASK_EXECUTE);
        addApprovedMetirc(MetricNames.EMIT_COUNT);
        addApprovedMetirc(MetricNames.DURATION);

        measureBuffer = new ArrayList<>(bufferSize);
        topologyOptimizer.init((String) conf.get(Config.TOPOLOGY_NAME), conf, this::getCachedDataAndClearBuffer);
        topologyOptimizer.start();
    }

    private List<MeasuredData> getCachedDataAndClearBuffer() {
        List<MeasuredData> ret = measureBuffer;
        measureBuffer = new ArrayList<>(bufferSize);
        return ret;
    }

    @Override
    protected void handleSelectedDataPoints(IMetricsConsumer.TaskInfo taskInfo, Collection<IMetricsConsumer.DataPoint> dataPoints) {
        Map<String, Object> ret = dataPoints.stream().collect(Collectors.toMap(p -> p.name, p -> p.value));
        //add to cache
        measureBuffer.add(new MeasuredData(taskInfo.srcComponentId, taskInfo.srcTaskId, taskInfo.timestamp, ret));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        topologyOptimizer.stop();
    }
}
