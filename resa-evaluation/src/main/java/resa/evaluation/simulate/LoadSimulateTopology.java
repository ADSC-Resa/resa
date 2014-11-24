package resa.evaluation.simulate;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static resa.util.ConfigUtil.readConfig;

/**
 * Created by ding on 14-7-29.
 */
public class LoadSimulateTopology {

    public static class SplitSentence extends BaseBasicBolt {

        private static final long serialVersionUID = 9182719848878455933L;

        public SplitSentence() {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String sentence = input.getStringByField("sentence");
            StringTokenizer tokenizer = new StringTokenizer(sentence.replaceAll("\\p{P}|\\p{S}", " "));
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().trim();
                if (!word.isEmpty()) {
                    collector.emit(Arrays.asList((Object) word.toLowerCase()));
                }
            }
            // Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        @Override
        public void cleanup() {
            System.out.println("Split cleanup");
        }
    }

    private static StormTopology createTopology(Map<String, Object> conf) {
        TopologyBuilder builder = new ResaTopologyBuilder();

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("redis.queue");
        builder.setSpout("input", new TASentenceSpout(host, port, queue), ConfigUtil.getInt(conf,
                "simulate.spout.parallelism", 1));

        int parallelism = ConfigUtil.getInt(conf, "simulate.bolt.split.parallelism", 1);
        builder.setBolt("split", new SplitSentence(), parallelism).shuffleGrouping("input")
                .setNumTasks(ConfigUtil.getInt(conf, "simulate.bolt.split.tasks", parallelism));

        parallelism = ConfigUtil.getInt(conf, "simulate.bolt.data-loader.parallelism", 1);
        int numTasks = ConfigUtil.getInt(conf, "simulate.bolt.data-loader.tasks", parallelism);
        int toMoveChunks = (int) (ConfigUtil.getDouble(conf, "simulate.bolt.data-loader.ratio", 1.0) * numTasks);
        Set<Integer> tasks = new HashSet<>();
        Random rand = new Random();
        while (tasks.size() < toMoveChunks) {
            tasks.add(rand.nextInt(numTasks));
        }
        System.out.println("To move tasks: " + tasks);
        conf.put("data-tasks", tasks.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.setBolt("data-loader", new SimulatedLoader(), parallelism).fieldsGrouping("split", new Fields("word"))
                .setNumTasks(numTasks);
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
            StormSubmitter.submitTopology(args[0], resaConfig, topology);
        }
    }

}
