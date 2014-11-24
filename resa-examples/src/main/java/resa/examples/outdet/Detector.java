package resa.examples.outdet;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-3-14.
 */
public class Detector implements IRichBolt {

    private static final double DEFAULT_PROJECTION_VALUE = Double.MAX_VALUE;
    public static final String OUTLIER_FIELD = "outlier";

    private static class Context {
        double[] projectionValues;
        int[] neighborCount;

        public Context(int size) {
            this.projectionValues = new double[size];
            Arrays.fill(projectionValues, DEFAULT_PROJECTION_VALUE);
            this.neighborCount = new int[size];
            Arrays.fill(neighborCount, 0);
        }
    }

    private int objectCount;
    private double maxNeighborDistance;
    private int minNeighborCount;
    private Map<Integer, Context> objectContext;
    private OutputCollector collector;

    public Detector(int objectCount, int minNeighborCount, double maxNeighborDistance) {
        this.objectCount = objectCount;
        this.minNeighborCount = minNeighborCount;
        this.maxNeighborDistance = maxNeighborDistance;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        objectContext = new HashMap<>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Integer projId = input.getIntegerByField(Projection.PROJECTION_ID_FIELD);
        Context context = objectContext.get(projId);
        if (context == null) {
            context = new Context(objectCount);
            objectContext.put(projId, context);
        }
        int objId = input.getIntegerByField(ObjectSpout.ID_FILED);
        double newProjValue = input.getDoubleByField(Projection.PROJECTION_VALUE_FIELD);
        int newNeighborCount = 0;
        boolean anyObjectMissing = false;
        BitSet outlier = new BitSet(objectCount);
        for (int i = 0; i < objectCount; i++) {
            // check whether if any objects missing
            if (context.projectionValues[i] == DEFAULT_PROJECTION_VALUE) {
                anyObjectMissing = true;
                continue;
            }
            if (i == objId) {
                continue;
            }
            boolean isNeighNow = isNeighbor(newProjValue, context.projectionValues[i]);
            boolean isNeighPast = context.projectionValues[objId] == DEFAULT_PROJECTION_VALUE ?
                    false : isNeighbor(context.projectionValues[objId], context.projectionValues[i]);
            if (isNeighPast && !isNeighNow) {
                context.neighborCount[i]--;
            } else if (!isNeighPast && isNeighNow) {
                // detect a new neighbor
                context.neighborCount[i]++;
            }
            outlier.set(i, context.neighborCount[i] < minNeighborCount);
            if (isNeighNow) {
                newNeighborCount++;
            }
        }
        context.projectionValues[objId] = newProjValue;
        context.neighborCount[objId] = newNeighborCount;
        outlier.set(objId, newNeighborCount < minNeighborCount);

        //Modified by tom, at the initial stat, we force this bolt to emit tuples (although fake) to Updater
        //if any objects missing, wait for it. This is used when system startup
        ///if (!anyObjectMissing) {
        collector.emit(input, new Values(objId, projId, outlier, input.getValueByField(ObjectSpout.TIME_FILED)));
        ///}
        collector.ack(input);
    }

    protected boolean isNeighbor(double projVal1, double projVal2) {
        return Math.abs(projVal1 - projVal2) <= maxNeighborDistance;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ObjectSpout.ID_FILED, Projection.PROJECTION_ID_FIELD,
                OUTLIER_FIELD, ObjectSpout.TIME_FILED));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
