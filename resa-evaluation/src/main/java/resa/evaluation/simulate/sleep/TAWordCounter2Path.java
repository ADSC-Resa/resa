package resa.evaluation.simulate.sleep;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Random;

/**
 * Created by ding on 14-1-27.
 */
public class TAWordCounter2Path extends TASleepBolt {

    private Random rand;
    private double p;

    public TAWordCounter2Path(IntervalSupplier sleep, double p) {
        super(sleep);
        this.p = p;
        rand = new Random();
    }

    @Override
    public void execute(Tuple tuple) {

        super.execute(tuple);
        String sid = tuple.getString(0);
        String word = tuple.getString(1);        
        
        double prob = rand.nextDouble();
        if (prob < this.p){
        	collector.emit("Bolt-P", tuple, new Values(sid, word));
        }
        else{
        	collector.emit("Bolt-NotP", new Values(word + "!!"));
        }        
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("Bolt-P", new Fields("sid", "word"));
        declarer.declareStream("Bolt-NotP", new Fields("word!"));
    }
}
