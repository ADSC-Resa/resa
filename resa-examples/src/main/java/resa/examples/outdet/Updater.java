package resa.examples.outdet;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by ding on 14-3-14.
 */
public class Updater implements IRichBolt {

    private OutputCollector collector;
    private Map<String, List<BitSet>> padding;
    private int projectionSize;

    public Updater(int projectionSize) {
        this.projectionSize = projectionSize;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        padding = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String key = input.getValueByField(ObjectSpout.TIME_FILED) + "-" + input.getValueByField(ObjectSpout.ID_FILED);
        List<BitSet> ret = padding.get(key);
        if (ret == null) {
            ret = new ArrayList<>();
            padding.put(key, ret);
        }
        ret.add((BitSet) input.getValueByField(Detector.OUTLIER_FIELD));
        if (ret.size() == projectionSize) {
            padding.remove(key);
            BitSet result = ret.get(0);
            ret.stream().forEach((bitSet) -> {
                if (result != bitSet) {
                    result.or(bitSet);
                }
            });
            // output
            //System.out.println(result);
//            result.stream().forEach((status) -> {
//                if (status == 0) {
//                    // output
//                    collector.emit(new Values());
//                }
//            });
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
