package resa.evaluation.topology.vld;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import resa.util.ConfigUtil;
import resa.util.Counter;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-7-3.
 * <p>
 * This beta version is Modified by Tom Fu, on April 2016
 * We mainly re-design the topology to remove those broadcasting issue (all grouping), here for experimental purpose
 */
public class AggregaterCharlie extends BaseRichBolt implements Constant {

    private class FrameContext {
        String frameId;
        int featDescCount = 0;
        int groupCount = 0;
        int curr = 0;
        Map<Integer, Counter> imageCounter = new HashMap<>();

        FrameContext(String frameId) {
            this.frameId = frameId;
        }

        FrameContext(String frameId, int featDescCount, int groupCount) {
            this.frameId = frameId;
            this.featDescCount = featDescCount;
            this.groupCount = groupCount;
        }

        void update(int[] matchedImages) {
            curr++;
            for (int i = 0; i < matchedImages.length; i += 2) {
                imageCounter.computeIfAbsent(matchedImages[i], (k) -> new Counter()).incAndGet(matchedImages[i + 1]);
            }
        }

        boolean isFinish() {
            return groupCount == curr && featDescCount != 0;
        }
    }

    private Map<String, FrameContext> pendingFrames;
    private OutputCollector collector;
    private double minPercentage;
//    private int indexPieces;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        pendingFrames = new HashMap<>();
        this.collector = collector;
        minPercentage = ConfigUtil.getDouble(stormConf, CONF_MATCH_RATIO, 0.5);
//        String srcComp = context.getThisSources().keySet().stream()
//                .filter(stream -> stream.get_streamId().equals(STREAM_MATCH_IMAGES))
//                .map(GlobalStreamId::get_componentId)
//                .findFirst().orElseThrow(() -> new RuntimeException("Can not find source comp"));
//        indexPieces = context.getComponentTasks(srcComp).size();
    }

    @Override
    public void execute(Tuple input) {
        FrameContext fCtx = pendingFrames.computeIfAbsent(input.getStringByField(FIELD_FRAME_ID),
                (k) -> new FrameContext(k, input.getIntegerByField(FIELD_FEATURE_CNT), input.getIntegerByField(SEND_GROUP_CNT)));

        fCtx.update((int[]) input.getValueByField(FIELD_MATCH_IMAGES));

        if (fCtx.isFinish()) {
            String out = fCtx.frameId + ":" + fCtx.imageCounter.entrySet().stream()
                    .filter(e -> (double) e.getValue().get() >  fCtx.featDescCount * minPercentage)
                    .map(e -> e.getKey().toString()).collect(Collectors.joining(","));
            System.out.println(out);
            // just for metrics output
            collector.emit(new Values(out));
            pendingFrames.remove(fCtx.frameId);
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("out"));
    }
}
