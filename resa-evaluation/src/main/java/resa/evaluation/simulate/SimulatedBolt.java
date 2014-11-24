package resa.evaluation.simulate;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by ding on 14-7-1.
 */
public class SimulatedBolt extends BaseRichBolt {

    public SimulatedBolt(double lambda) {
        this.lambda = lambda;
    }

    private OutputCollector collector;
    private double lambda;
    private long bound;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
        bound = (long) (lambda * 10 * 1_000_000L);
    }

    @Override
    public void execute(Tuple tuple) {
        long cost;
        long exp = Math.min((long) (-Math.log(Math.random()) * lambda) * 1_000_000L, bound);
        long now = System.nanoTime();
        do {
            for (int i = 0; i < 10; i++) {
                Math.atan(Math.sqrt(Math.random() * Integer.MAX_VALUE));
            }
        } while ((cost = (System.nanoTime() - now)) > 0 && cost < exp);
        collector.emit(tuple, tuple.getValues());
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text"));
    }

}
