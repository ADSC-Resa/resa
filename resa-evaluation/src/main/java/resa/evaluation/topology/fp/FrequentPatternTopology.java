package resa.evaluation.topology.fp;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;

import static resa.evaluation.topology.tomVLD.StormConfigManager.getInt;
import static resa.evaluation.topology.tomVLD.StormConfigManager.readConfig;

/**
 * Created by ding on 14-6-6.
 */
public class FrequentPatternTopology implements Constant {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = readConfig(args[0]);

//        TopologyBuilder builder = new WritableTopologyBuilder();
        TopologyBuilder builder = new ResaTopologyBuilder();

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("redis.queue");

        builder.setSpout("input", new SentenceSpout(host, port, queue), ConfigUtil.getInt(conf, "fp.spout.parallelism", 1));

        builder.setBolt("generator", new PatternGenerator(), ConfigUtil.getInt(conf, "fp.generator.parallelism", 1))
                .shuffleGrouping("input")
                .setNumTasks(ConfigUtil.getInt(conf, "fp.generator.tasks", 1));
        builder.setBolt("detector", new Detector(), ConfigUtil.getInt(conf, "fp.detector.parallelism", 1))
                //doneTODO:
                //.fieldsGrouping("generator", new Fields(PATTERN_FIELD))
                //.fieldsGrouping("detector", FEEDBACK_STREAM, new Fields(PATTERN_FIELD))
                .directGrouping("generator")
                .directGrouping("detector", FEEDBACK_STREAM)
                .setNumTasks(ConfigUtil.getInt(conf, "fp.detector.tasks", 1));

        builder.setBolt("reporter", new PatternReporter(), ConfigUtil.getInt(conf, "fp.reporter.parallelism", 1))
                .fieldsGrouping("detector", REPORT_STREAM, new Fields(PATTERN_FIELD))
                .setNumTasks(ConfigUtil.getInt(conf, "fp.reporter.tasks", 1));


        int numWorkers = ConfigUtil.getInt(conf, "fp-worker.count", 1);
        conf.setNumWorkers(numWorkers);
        conf.setMaxSpoutPending(ConfigUtil.getInt(conf, "fp-MaxSpoutPending", 0));
        conf.setStatsSampleRate(1.0);

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);

        if (ConfigUtil.getBoolean(conf, "fp.metric.resa", false)) {
            resaConfig.addDrsSupport();
            resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
            System.out.println("ResaMetricsCollector is registered");
        }

        StormSubmitter.submitTopology("resa-fp-test", resaConfig, builder.createTopology());
    }

}
