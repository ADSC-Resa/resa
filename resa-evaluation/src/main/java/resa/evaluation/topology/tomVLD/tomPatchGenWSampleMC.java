package resa.evaluation.topology.tomVLD;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.List;
import java.util.Map;

import static resa.evaluation.topology.tomVLD.Constants.*;

/**
 * Created by Tom FU.
 * This is the patch Generator of the original tomVLDtopology, with intelligent multicast of raw frames, need
 * complicated partition and grouping approach (direct grouping). *
 */
public class tomPatchGenWSampleMC extends BaseRichBolt {
    OutputCollector collector;
    private int taskIndex;
    private int taskCnt;

    String targetComponentName;
    List<Integer> targetComponentTasks;

    private int sampleFrames;

    public tomPatchGenWSampleMC(String targetComponentName) {
        this.targetComponentName = targetComponentName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.taskIndex = topologyContext.getThisTaskIndex();
        this.taskCnt = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        targetComponentTasks = topologyContext.getComponentTasks(targetComponentName);

        sampleFrames = ConfigUtil.getInt(map, "sampleFrames", 1);
    }

    @Override
    public void execute(Tuple tuple) {

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);

        //TODO get params from config map
        double fx = .25, fy = .25;
        double fsx = .5, fsy = .5;
        //double fsx = .4, fsy = .4;

        int W = sMat.getCols(), H = sMat.getRows();
        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);
        int totalPatchCount = 0;
        for (int x = 0; x + w <= W; x += dx)
            for (int y = 0; y + h <= H; y += dy)
                totalPatchCount++;

        //send patch
        ///notice the bug, the raw frame stream is also not necessary to send to patchProcess bolt
        if (frameId % sampleFrames == 0) {
            //send raw frames
            for (int i = 0; i < targetComponentTasks.size(); i++) {
                int tID = targetComponentTasks.get(i);
                if (tID % this.taskCnt == this.taskIndex) {
                    collector.emitDirect(tID, RAW_FRAME_STREAM, tuple, new Values(frameId, sMat, totalPatchCount));
                }
            }

            int pCnt = 0;
            for (int x = 0; x + w <= W; x += dx) {
                for (int y = 0; y + h <= H; y += dy) {
                    if (pCnt % this.taskCnt == this.taskIndex) {
                        Serializable.PatchIdentifier identifier = new
                                Serializable.PatchIdentifier(frameId, new Serializable.Rect(x, y, w, h));
                        collector.emit(PATCH_STREAM, tuple, new Values(identifier, totalPatchCount));
                    }
                    pCnt++;
                }
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PATCH_STREAM, new Fields(FIELD_PATCH_IDENTIFIER, FIELD_PATCH_COUNT));
        //EmitDirect
        outputFieldsDeclarer.declareStream(RAW_FRAME_STREAM, true, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_PATCH_COUNT));
    }
}
