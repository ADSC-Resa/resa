package resa.examples.wc;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class WordCountTopology {

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

    public static class WordCount extends BaseBasicBolt {
        private static final long serialVersionUID = 4905347466083499207L;
        private int numBuckets = 6;
        private Map<String, Integer> counters;

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            super.prepare(stormConf, context);
            counters = (Map<String, Integer>) context.getTaskData("words");
            if (counters == null) {
                counters = new HashMap<>();
                context.setTaskData("words", counters);
            }
            int interval = Utils.getInt(stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
            context.registerMetric("number-words", this::getNumWords, interval);
        }

        private long getNumWords() {
            //counters.rotate();
            return counters.size();
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getStringByField("word");
            Integer count = counters.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            counters.put(word, count);
            //collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public void cleanup() {
            System.out.println("Word Counter cleanup");
        }

    }

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
            builder.setSpout("say", new TASentenceSpout(host, port, queue),
                    ConfigUtil.getInt(conf, "spout.parallelism", 1));
        }
        builder.setBolt("split", new SplitSentence(), ConfigUtil.getInt(conf, "split.parallelism", 1))
                .shuffleGrouping("say");
        builder.setBolt("counter", new WordCount(), ConfigUtil.getInt(conf, "counter.parallelism", 1))
                .fieldsGrouping("split", new Fields("word"));
        resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
    }
}
