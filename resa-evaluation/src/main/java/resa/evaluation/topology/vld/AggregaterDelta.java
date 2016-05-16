package resa.evaluation.topology.vld;

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
public class AggregaterDelta extends BaseRichBolt implements Constant {

    private class FrameContext {
        int frameId;
        int patchCount = 0;
        int featDescCount = 0;
        int curr = 0;
        Map<Integer, Counter> imageCounter = new HashMap<>();

        FrameContext(int frameId) {
            this.frameId = frameId;
        }

        FrameContext(int frameId, int patchCount) {
            this.frameId = frameId;
            this.patchCount = patchCount;
        }

        void update(int[] matchedImages, int featDescCount) {
            curr++;
            this.featDescCount += featDescCount;
            for (int i = 0; i < matchedImages.length; i += 2) {
                imageCounter.computeIfAbsent(matchedImages[i], (k) -> new Counter()).incAndGet(matchedImages[i + 1]);
            }
        }

        boolean isFinish() {
            return patchCount == curr;
        }
    }

    private Map<Integer, FrameContext> pendingFrames;
    private OutputCollector collector;
    private double minPercentage;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        pendingFrames = new HashMap<>();
        this.collector = collector;
        minPercentage = ConfigUtil.getDouble(stormConf, CONF_MATCH_RATIO, 0.5);
    }

    @Override
    public void execute(Tuple input) {
        FrameContext fCtx = pendingFrames.computeIfAbsent(input.getIntegerByField(FIELD_FRAME_ID),
                (k) -> new FrameContext(k, input.getIntegerByField(FIELD_PATCH_COUNT)));

        fCtx.update((int[]) input.getValueByField(FIELD_MATCH_IMAGES), input.getIntegerByField(FIELD_FEATURE_CNT));

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
