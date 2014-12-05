package resa.evaluation.topology.vld;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ResaConfig;

import java.io.File;

import static resa.util.ConfigUtil.getInt;
import static resa.util.ConfigUtil.readConfig;

/**
 * Created by ding on 14-7-3.
 */
public class DectationTopology implements Constant {

    private static StormTopology createTopology(Config conf) {
        TopologyBuilder builder = new ResaTopologyBuilder();

        String host = (String) conf.get("redis.host");
        int port = ((Number) conf.get("redis.port")).intValue();
        String queue = (String) conf.get("redis.queue");
        builder.setSpout("image-input", new ImageSource(host, port, queue), getInt(conf, "vd.spout.parallelism", 1));

        builder.setBolt("feat-ext", new FeatureExtracter(), getInt(conf, "vd.feat-ext.parallelism", 1))
                .shuffleGrouping("image-input", STREAM_IMG_OUTPUT)
                .setNumTasks(getInt(conf, "vd.feat-ext.tasks", 1));
        builder.setBolt("matcher", new Matcher(), getInt(conf, "vd.matcher.parallelism", 1))
                .allGrouping("feat-ext", STREAM_FEATURE_DESC)
                .setNumTasks(getInt(conf, "vd.matcher.tasks", 1));
        builder.setBolt("aggregater", new Aggregater(), getInt(conf, "vd.aggregater.parallelism", 1))
                .fieldsGrouping("feat-ext", STREAM_FEATURE_COUNT, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping("matcher", STREAM_MATCH_IMAGES, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, "vd.aggregater.tasks", 1));
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
            resaConfig.addDrsSupport();
            resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
            StormSubmitter.submitTopology(args[0], resaConfig, topology);
        }
    }

}
