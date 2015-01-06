package resa.examples.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import resa.examples.wc.RandomSentenceSpout;
import resa.examples.wc.TASentenceSpout;
import resa.util.ConfigUtil;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * Created by ding on 14/12/30.
 */
public class TridentWordCount {

    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            StringTokenizer tokenizer = new StringTokenizer(sentence.replaceAll("\\p{P}|\\p{S}", " "));
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().trim();
                if (!word.isEmpty()) {
                    collector.emit(Arrays.asList((Object) word.toLowerCase()));
                }
            }
        }
    }

    public static StormTopology buildTopology(Config conf) {
        IRichSpout spout;
        if (!ConfigUtil.getBoolean(conf, "spout.redis", false)) {
            spout = new RandomSentenceSpout();
        } else {
            String host = (String) conf.get("redis.host");
            int port = ((Number) conf.get("redis.port")).intValue();
            String queue = (String) conf.get("redis.queue");
            spout = new TASentenceSpout(host, port, queue);
        }
        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout", spout).parallelismHint(4)
                .each(new Fields("sentence"), new Split(), new Fields("word")).parallelismHint(8)
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(8);

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(conf));
        } else {
            conf.setNumWorkers(Integer.parseInt(args[1]));
            conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 100);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(conf));
        }
    }

}
