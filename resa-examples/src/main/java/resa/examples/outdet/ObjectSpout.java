package resa.examples.outdet;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import resa.topology.RedisQueueSpout;

import java.util.Arrays;
import java.util.Collections;

/**
 * Created by ding on 14-3-14.
 */
public class ObjectSpout extends RedisQueueSpout {

    public static final String ID_FILED = "id";
    public static final String VECTOR_FILED = "vector";
    public static final String TIME_FILED = "time";

    private final int objectCount;

    public ObjectSpout(String host, int port, String queue, int objectCount) {
        super(host, port, queue);
        this.objectCount = objectCount;
    }

    @Override
    protected void emitData(Object data) {
        String[] tmp = ((String) data).split("[|]");
        double[] v = Arrays.stream(tmp[2].split(",")).mapToDouble((str) -> Double.parseDouble(str)).toArray();
        Integer objId = (int) (Long.parseLong(tmp[0]) % objectCount);
        Long timestamp = Long.parseLong(tmp[1]);
        collector.emit(new Values(objId, v, timestamp), Collections.singletonMap(objId, timestamp));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ID_FILED, VECTOR_FILED, TIME_FILED));
    }
}
