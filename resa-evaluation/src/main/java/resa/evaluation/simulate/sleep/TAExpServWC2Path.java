package resa.evaluation.simulate.sleep;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class TAExpServWC2Path {


    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);

        TopologyBuilder builder = new ResaTopologyBuilder();

        int numWorkers = ConfigUtil.getInt(conf, "a3-worker.count", 1);
        int numAckers = ConfigUtil.getInt(conf, "a3-acker.count", 1);

        resaConfig.setNumWorkers(numWorkers);
        resaConfig.setNumAckers(numAckers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("a3-redis.queue");

        int defaultTaskNum = ConfigUtil.getInt(conf, "a3-task.default", 10);

        builder.setSpout("spout2Path", new TASentenceSpout2Path(host, port, queue,
                        ConfigUtil.getDouble(conf, "a3-spout.prob", 1.0)),
                ConfigUtil.getInt(conf, "a3-spout.parallelism", 1));

        double splitP_mu = ConfigUtil.getDouble(conf, "a3-splitP.mu", 1.0);
        double splitNotP_mu = ConfigUtil.getDouble(conf, "a3-splitNotP.mu", 1.0);

        builder.setBolt("splitP",
                new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / splitP_mu)),
                ConfigUtil.getInt(conf, "a3-splitP.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("spout2Path", "Bolt-P");

        builder.setBolt("splitNotP",
                new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / splitNotP_mu)),
                ConfigUtil.getInt(conf, "a3-splitNotP.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("spout2Path", "Bolt-NotP");

        double counter_mu = ConfigUtil.getDouble(conf, "a3-counter.mu", 1.0);
        builder.setBolt("counter", new TAWordCounter(() -> (long) (-Math.log(Math.random()) * 1000.0 / counter_mu)),
                ConfigUtil.getInt(conf, "a3-counter.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("splitP").shuffleGrouping("splitNotP");

        if (ConfigUtil.getBoolean(conf, "a3-metric.resa", false)) {
            resaConfig.addOptimizeSupport();
            resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
            System.out.println("ResaMetricsCollector is registered");
        }

        if (ConfigUtil.getBoolean(conf, "a3-metric.redis", true)) {
            resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
            System.out.println("RedisMetricsCollector is registered");
        }

        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
    }
}
