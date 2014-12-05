package resa.examples.outdet;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Created by ding on 14-3-17.
 */
public class OutlierDetectionTop {

    public static List<double[]> generateRandomVectors(int dimension, int vectorCount) {
        Random rand = new Random();
        return Stream.generate(() -> {
            double[] v = DoubleStream.generate(rand::nextGaussian).limit(dimension).toArray();
            double sum = Math.sqrt(Arrays.stream(v).map((d) -> d * d).sum());
            return Arrays.stream(v).map((d) -> d / sum).toArray();
        }).limit(vectorCount).collect(Collectors.toList());
    }

    public static void main(String[] args) throws Exception {
        Config conf = ConfigUtil.readConfig(new File(args[1]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);

        TopologyBuilder builder = new ResaTopologyBuilder();

        int numWorkers = ConfigUtil.getInt(conf, "a-worker.count", 1);
        int numAckers = ConfigUtil.getInt(conf, "a-acker.count", 1);

        resaConfig.setNumWorkers(numWorkers);
        resaConfig.setNumAckers(numAckers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("redis.queue");

        int defaultTaskNum = ConfigUtil.getInt(conf, "a-task.default", 10);
        //set spout
        int objectCount = ConfigUtil.getIntThrow(conf, "a-spout.object.size");
        builder.setSpout("objectSpout",
                new ObjectSpout(host, port, queue, objectCount),
                ConfigUtil.getInt(conf, "a-spout.parallelism", 1));

        List<double[]> randVectors = generateRandomVectors(ConfigUtil.getIntThrow(conf, "a-projection.dimension"),
                ConfigUtil.getIntThrow(conf, "a-projection.size"));

        builder.setBolt("projection",
                new Projection(new ArrayList<>(randVectors)), ConfigUtil.getInt(conf, "a-projection.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("objectSpout");

        int minNeighborCount = ConfigUtil.getIntThrow(conf, "a-detector.neighbor.count.min");
        double maxNeighborDistance = ConfigUtil.getDoubleThrow(conf, "a-detector.neighbor.distance.max");
        builder.setBolt("detector",
                new Detector(objectCount, minNeighborCount, maxNeighborDistance),
                ConfigUtil.getInt(conf, "a-detector.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .fieldsGrouping("projection", new Fields(Projection.PROJECTION_ID_FIELD));

        builder.setBolt("updater",
                new Updater(randVectors.size()), ConfigUtil.getInt(conf, "a-updater.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .fieldsGrouping("detector", new Fields(ObjectSpout.TIME_FILED, ObjectSpout.ID_FILED));

        if (ConfigUtil.getBoolean(conf, "a-metric.resa", false)) {
            resaConfig.addDrsSupport();
            resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
            System.out.println("ResaMetricsCollector is registered");
        }

        if (ConfigUtil.getBoolean(conf, "a-metric.redis", true)) {
            resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
            System.out.println("RedisMetricsCollector is registered");
        }

        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
    }

}
