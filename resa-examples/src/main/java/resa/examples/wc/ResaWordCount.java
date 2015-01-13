package resa.examples.wc;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;

/**
 * Created by ding on 15/1/6.
 */
public class ResaWordCount {

    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[1]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);

        TopologyBuilder builder = new ResaTopologyBuilder();

        if (!ConfigUtil.getBoolean(conf, "spout.redis", false)) {
            builder.setSpout("say", new RandomSentenceSpout(), ConfigUtil.getInt(conf, "spout.parallelism", 1));
        } else {
            String host = (String) conf.get("redis.host");
            int port = ((Number) conf.get("redis.port")).intValue();
            String queue = (String) conf.get("redis.queue");
            builder.setSpout("say", new RedisSentenceSpout(host, port, queue),
                    ConfigUtil.getInt(conf, "spout.parallelism", 1));
        }
        builder.setBolt("split", new WordCountTopology.SplitSentence(), ConfigUtil.getInt(conf, "split.parallelism", 1))
                .shuffleGrouping("say");
        builder.setBolt("counter", new WordCountTopology.WordCount(), ConfigUtil.getInt(conf, "counter.parallelism", 1))
                .fieldsGrouping("split", new Fields("word"));
        // add drs component
        resaConfig.addDrsSupport();
        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
    }

}
