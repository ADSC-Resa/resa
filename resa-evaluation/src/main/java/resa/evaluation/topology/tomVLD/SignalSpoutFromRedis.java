package resa.evaluation.topology.tomVLD;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import resa.topology.RedisQueueSpout;

import java.util.Map;

import static resa.evaluation.topology.tomVLD.Constants.FIELD_SIGNAL_TYPE;
import static resa.evaluation.topology.tomVLD.Constants.SIGNAL_STREAM;

/**
 * Updated on Aug 5,  the way to handle frame sampling issue is changed, this is pre-processed by the spout not to
 * send out unsampled frames to the patch generation bolt.
 * also binding to ImageSenderFox!
 */
public class SignalSpoutFromRedis extends RedisQueueSpout {

    public SignalSpoutFromRedis(String host, int port, String queue) {
        super(host, port, queue, true);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
    }

    @Override
    protected void emitData(Object data) {
        long now = System.currentTimeMillis();
        collector.emit(SIGNAL_STREAM, new Values(data), now);
        System.out.printf("SendoutSignal: " + now + "," + data.toString());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SIGNAL_STREAM, new Fields(FIELD_SIGNAL_TYPE));
    }
}
