package resa.evaluation.simulate;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import resa.util.ConfigUtil;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Created by ding on 14-7-29.
 */
public class SimulatedLoader extends BaseBasicBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        long currTime = System.currentTimeMillis();
        boolean needLoadData = Stream.of(((String) stormConf.get("data-tasks")).split(",")).mapToInt(Integer::parseInt)
                .anyMatch(i -> i == context.getThisTaskIndex());
        if (needLoadData) {
            long sleep = ConfigUtil.getIntThrow(stormConf, "simulate.data-loader.sleep");
//            int rand = (int) (Math.random() * sleep * 0.2);
//            if (rand % 2 == 0) {
//                sleep = sleep + rand;
//            } else {
//                sleep = sleep - rand;
//            }
            Utils.sleep(sleep);
        }
        long cost = System.currentTimeMillis() - currTime;
        context.getSharedExecutor().submit(() -> writeTimeToRedis(stormConf, context, cost));
    }

    private void writeTimeToRedis(Map conf, TopologyContext context, long time) {
        Jedis jedis = new Jedis((String) conf.get("redis.host"), ConfigUtil.getInt(conf, "redis.port", 6379));
        jedis.set(context.getStormId() + "-" + context.getThisComponentId() + "-" + context.getThisTaskId()
                + "-start-time", time < 1000 ? "zero" : String.valueOf(time));
        jedis.disconnect();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
//        long now = System.currentTimeMillis();
//        do {
        for (int i = 0; i < 10; i++) {
            Math.atan(Math.sqrt(Math.random() * Integer.MAX_VALUE) * Math.PI);
        }
//        } while (System.currentTimeMillis() - now > 1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
