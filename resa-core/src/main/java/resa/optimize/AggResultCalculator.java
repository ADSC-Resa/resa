package resa.optimize;

import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.ExecutorDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.metrics.MeasuredData;
import resa.metrics.MetricNames;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-3-4.
 * Note:
 * Recv-Queue arrival count includes ack for each message
 * When calculate sum and average, need to adjust (sum - #message, average - 1) for accurate value.
 */
public class AggResultCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(AggResultCalculator.class);

    protected Iterable<MeasuredData> dataStream;
    private StormTopology rawTopo;
    private final Map<Integer, AggResult> task2Result = new HashMap<>();
    private final Map<String, AggResult[]> results = new HashMap<>();
    private final Set<Integer> firstTasks;

    public AggResultCalculator(Iterable<MeasuredData> dataStream, Map<String, List<ExecutorDetails>> comp2Executors,
                               StormTopology rawTopo) {
        this.dataStream = dataStream;
        this.rawTopo = rawTopo;
        comp2Executors.forEach((comp, exeList) -> {
            AggResult[] result;
            if (rawTopo.get_spouts().containsKey(comp)) {
                result = new SpoutAggResult[exeList.size()];
                for (int i = 0; i < result.length; i++) {
                    result[i] = createTaskIndex(new SpoutAggResult(), exeList.get(i));
                }
            } else {
                result = new BoltAggResult[exeList.size()];
                for (int i = 0; i < result.length; i++) {
                    result[i] = createTaskIndex(new BoltAggResult(), exeList.get(i));
                }
            }
            results.put(comp, result);
        });
        firstTasks = comp2Executors.values().stream().flatMap(e -> e.stream()).map(ExecutorDetails::getStartTask)
                .collect(Collectors.toSet());
    }

    private AggResult createTaskIndex(AggResult result, ExecutorDetails e) {
        IntStream.rangeClosed(e.getStartTask(), e.getEndTask()).forEach((task) -> task2Result.put(task, result));
        return result;
    }

    private AggResult parse(MeasuredData measuredData, AggResult dest) {
        // parse send queue and recv queue first
        measuredData.data.computeIfPresent(MetricNames.SEND_QUEUE, (comp, data) -> {
            parseQueueResult((Map<String, Number>) data, dest.getSendQueueResult());
            return data;
        });
        measuredData.data.computeIfPresent(MetricNames.RECV_QUEUE, (comp, data) -> {
            parseQueueResult((Map<String, Number>) data, dest.getRecvQueueResult());
            return data;
        });
        if (firstTasks.contains(measuredData.task)) {
            measuredData.data.computeIfPresent(MetricNames.DURATION, (comp, data) -> {
                dest.addDuration(((Number) data).longValue());
                return data;
            });
        }
        if (rawTopo.get_spouts().containsKey(measuredData.component)) {
            Map<String, Object> data = (Map<String, Object>) measuredData.data.get(MetricNames.COMPLETE_LATENCY);
            if (data != null) {
                data.forEach((stream, elementStr) -> {
                    String[] elements = ((String) elementStr).split(",");
                    int cnt = Integer.valueOf(elements[0]);
                    if (cnt > 0) {
                        double val = Double.valueOf(elements[1]);
                        double val_2 = Double.valueOf(elements[2]);
                        ((SpoutAggResult) dest).getCompletedLatency().computeIfAbsent(stream, (k) -> new CntMeanVar())
                                .addAggWin(cnt, val, val_2);
                    }
                });
            }
        } else {
            Map<String, Object> data = (Map<String, Object>) measuredData.data.get(MetricNames.TASK_EXECUTE);
            if (data != null) {
                data.forEach((stream, elementStr) -> {
                    String[] elements = ((String) elementStr).split(",");
                    int cnt = Integer.valueOf(elements[0]);
                    if (cnt > 0) {
                        double val = Double.valueOf(elements[1]);
                        double val_2 = Double.valueOf(elements[2]);
                        ((BoltAggResult) dest).getTupleProcess().computeIfAbsent(stream, (k) -> new CntMeanVar())
                                .addAggWin(cnt, val, val_2);
                    }
                });
            }
        }
        return dest;
    }

    private void parseQueueResult(Map<String, Number> queueMetrics, QueueAggResult queueResult) {
        long totalArrivalCnt = queueMetrics.getOrDefault("totalCount", Integer.valueOf(0)).longValue();
        if (totalArrivalCnt > 0) {
            int sampleCnt = queueMetrics.getOrDefault("sampleCount", Integer.valueOf(0)).intValue();
            long totalQLen = queueMetrics.getOrDefault("totalQueueLen", Integer.valueOf(0)).longValue();
            // long duration = queueMetrics.getOrDefault("duration", Integer.valueOf(0)).longValue();
            queueResult.add(totalArrivalCnt, totalQLen, sampleCnt);
        }
    }

    public void calCMVStat() {
        int count = 0;
        for (MeasuredData measuredData : dataStream) {
            ///Real example
            //69) "objectSpout:4->{\"receive\":{\"sampleCount\":209,\"totalQueueLen\":212,\"totalCount\":4170},\"complete-latency\":{\"default\":\"2086,60635.0,3382707.0\"},\"sendqueue\":{\"sampleCount\":420,\"totalQueueLen\":424,\"totalCount\":8402}}"
            //70) "projection:7->{\"receive\":{\"sampleCount\":52,\"totalQueueLen\":53,\"totalCount\":1052},\"sendqueue\":{\"sampleCount\":2152,\"totalQueueLen\":4514,\"totalCount\":43052},\"execute\":{\"objectSpout:default\":\"525,709.4337659999997,1120.8007487084597\"}}"
            //71) "detector:3->{\"receive\":{\"sampleCount\":2769,\"totalQueueLen\":6088758,\"totalCount\":55416},\"sendqueue\":{\"sampleCount\":8921,\"totalQueueLen\":11476,\"totalCount\":178402},\"execute\":{\"projection:default\":\"49200,5167.623237000047,721.6383647758853\"}}"
            //73) "updater:9->{\"receive\":{\"sampleCount\":3921,\"totalQueueLen\":5495,\"totalCount\":78436},\"sendqueue\":{\"sampleCount\":4001,\"totalQueueLen\":4336,\"totalCount\":80002},\"execute\":{\"detector:default\":\"40000,1651.7782049999894,182.68124734051045\"}}"
            AggResult car = task2Result.get(measuredData.task);
            parse(measuredData, car);
            count++;
        }
        LOG.info("calCMVStat, processed measuredData size: " + count);
    }

    public Map<String, AggResult[]> getResults() {
        return results;
    }

    public Map<String, AggResult[]> getSpoutResults() {
        return results.entrySet().stream().filter(e -> rawTopo.get_spouts().containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Map<String, AggResult[]> getBoltResults() {
        return results.entrySet().stream().filter(e -> rawTopo.get_bolts().containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
