package resa.evaluation.simulate;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;
import java.util.Map;

import static resa.util.ConfigUtil.readConfig;

/**
 * Created by ding on 14-7-17.
 */
public class SimulatedTopology {

    private static StormTopology createTopology(Map<String, Object> conf) {
        TopologyBuilder builder = new ResaTopologyBuilder();

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("redis.queue");

        builder.setSpout("input", new SimulatedSpout(host, port, queue), ConfigUtil.getInt(conf,
                "simulate.spout.parallelism", 1));
        int boltCount = ConfigUtil.getInt(conf, "simulate.bolt.count", 1);
        long totalComputeTime = 0;
        for (int i = 1; i <= boltCount; i++) {
            double computeTime = ConfigUtil.getDouble(conf, "simulate.bolt." + i + ".compute-time", 1);
            int parallelism = ConfigUtil.getInt(conf, "simulate.bolt." + i + ".parallelism", 1);
            int numTasks = ConfigUtil.getInt(conf, "simulate.bolt." + i + ".tasks", parallelism);
            String lastComp = i == 1 ? "input" : "bolt-" + (i - 1);
            builder.setBolt("bolt-" + i, new SimulatedBolt(computeTime), parallelism).shuffleGrouping(lastComp)
                    .setNumTasks(numTasks);
            totalComputeTime += computeTime;
        }
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, totalComputeTime * 3);
        return builder.createTopology();
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config conf = readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }
        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);
        StormTopology topology = createTopology(conf);
        if (args[0].equals("[local]")) {
            resaConfig.setDebug(false);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("local", resaConfig, topology);
        } else {
            resaConfig.addOptimizeSupport();
            resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
            StormSubmitter.submitTopology(args[0], resaConfig, topology);
        }
    }

}
