package resa.evaluation.simulate;

import backtype.storm.tuple.Values;
import resa.topology.RedisQueueSpout;


/**
 * Created by ding on 14-6-5.
 */
public class SimulatedSpout extends RedisQueueSpout {

    public SimulatedSpout(String host, int port, String queue) {
        super(host, port, queue);
    }

    @Override
    protected void emitData(Object data) {
        String text = (String) data;
        StringBuilder sb = new StringBuilder(text.length() * 1000);
        for (int i = 0; i < 1000; i++) {
            sb.append(text);
        }
        collector.emit(new Values(sb.toString()), "");
    }

}
