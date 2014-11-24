package resa.examples.wc;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class RandomSentenceSpout extends BaseRichSpout {

    private static final long serialVersionUID = 3963979649966518694L;

    private transient SpoutOutputCollector _collector;
    private transient Random _rand;

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(_rand.nextInt(10) + 10);
        String[] sentences = new String[]{
                "the cow jumped over the moon",
                "an apple a day keeps the doctor away",
                "four score and seven years ago",
                "snow white and the seven dwarfs",
                "i am at two with nature",
                "the latest news and headlines from Yahoo! news",
                "breaking news latest news and current news",
                "the latest news from across canada and around the world",
                "get top headlines on international business news",
                "cnn delivers the latest breaking news and information on the latest top stories",
                "get breaking national and world news broadcast video coverage and exclusive interviews"};
        String sentence = sentences[_rand.nextInt(sentences.length)];
        _collector.emit(new Values(sentence), UUID.randomUUID().toString());
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

}