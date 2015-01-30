package resa.evaluation.trident;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ICommitterTridentSpout;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

public class OneSentencePerBatchSpout implements ITridentSpout<Object> {

    public static String[] SENTENCES = new String[]{
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

    private static final long serialVersionUID = 3963979649966518694L;

    @Override
    @SuppressWarnings("unchecked")
    public BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return new BatchCoordinator() {
            @Override
            public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
                return null;
            }

            @Override
            public void success(long txid) {
            }

            @Override
            public boolean isReady(long txid) {
                return true;
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public Emitter<Object> getEmitter(String txStateId, Map conf, TopologyContext context) {
        int taskIndex = context.getThisTaskIndex();
        int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        return new ICommitterTridentSpout.Emitter() {
            private Random rand = new Random();

            @Override
            public void commit(TransactionAttempt attempt) {
            }

            @Override
            public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
                if (tx.getTransactionId() % numTasks == taskIndex) {
                    collector.emit(Arrays.asList(SENTENCES[rand.nextInt(SENTENCES.length)]));
                }
            }

            @Override
            public void success(TransactionAttempt tx) {
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("sentence");
    }
}