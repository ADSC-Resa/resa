package resa.evaluation.simulate;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import resa.metrics.RedisMetricsCollector;
import resa.examples.wc.TASentenceSpout;
import resa.examples.wc.WordCountTopology;
import resa.evaluation.topology.WritableTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static resa.util.ConfigUtil.readConfig;

/**
 * Created by ding on 14-7-29.
 */
public class LoadDataTopology {

    private static StormTopology createTopology(Map<String, Object> conf) {
        TopologyBuilder builder = new WritableTopologyBuilder();

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("redis.queue");
        builder.setSpout("input", new TASentenceSpout(host, port, queue), ConfigUtil.getInt(conf,
                "simulate.spout.parallelism", 1));

        int parallelism = ConfigUtil.getInt(conf, "simulate.bolt.split.parallelism", 1);
        builder.setBolt("split", new WordCountTopology.SplitSentence(), parallelism).shuffleGrouping("input")
                .setNumTasks(ConfigUtil.getInt(conf, "simulate.bolt.split.tasks", parallelism));

        parallelism = ConfigUtil.getInt(conf, "simulate.bolt.data-loader.parallelism", 1);
        builder.setBolt("data-loader", new HdfsDataLoader(), parallelism).fieldsGrouping("split", new Fields("word"))
                .setNumTasks(ConfigUtil.getInt(conf, "simulate.bolt.data-loader.tasks", parallelism));
        //conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, totalComputeTime * 3);
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        Config conf = readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }
        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);
        StormTopology topology = createTopology(resaConfig);
        if (args[0].equals("[local]")) {
            resaConfig.setDebug(false);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("local", resaConfig, topology);
        } else {
//            resaConfig.addOptimizeSupport();
            resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
            List<Double> dataSizes = Files.readAllLines(Paths.get(args[2])).stream().map(String::trim)
                    .filter(s -> !s.isEmpty()).map(Double::valueOf).collect(Collectors.toList());
            resaConfig.put("dataSizes", dataSizes);
            StormSubmitter.submitTopology(args[0], resaConfig, topology);
        }
    }

}
