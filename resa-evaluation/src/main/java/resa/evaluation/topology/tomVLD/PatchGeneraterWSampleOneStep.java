package resa.evaluation.topology.tomVLD;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;

import java.util.Map;

import static resa.evaluation.topology.tomVLD.Constants.*;

/**
 * Created by Intern04 on 5/8/2014.
 */
public class PatchGeneraterWSampleOneStep extends BaseRichBolt {
    OutputCollector collector;
    private int sampleFrames;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        sampleFrames = ConfigUtil.getInt(map, "sampleFrames", 1);
    }

    @Override
    public void execute(Tuple tuple) {

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        opencv_core.IplImage fk = new opencv_core.IplImage();

        collector.emit(RAW_FRAME_STREAM, tuple, new Values(frameId, sMat));

        double fx = .25, fy = .25;
        double fsx = .5, fsy = .5;

        int W = sMat.getCols(), H = sMat.getRows();
        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);
        int patchCount = 0;
        for (int x = 0; x + w <= W; x += dx)
            for (int y = 0; y + h <= H; y += dy)
                patchCount++;

        if (frameId % sampleFrames == 0) {
            for (int x = 0; x + w <= W; x += dx) {
                for (int y = 0; y + h <= H; y += dy) {
                    Serializable.Rect rect = new Serializable.Rect(x, y, w, h);

                    opencv_core.Mat pMat = new opencv_core.Mat(sMat.toJavaCVMat(), rect.toJavaCVRect());
                    Serializable.Mat pSMat = new Serializable.Mat(pMat);
                    Serializable.PatchIdentifierMat subPatchMat = new Serializable.PatchIdentifierMat(frameId, rect, pSMat);

                    collector.emit(PATCH_FRAME_STREAM, tuple, new Values(frameId, subPatchMat, patchCount));
                }
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
        outputFieldsDeclarer.declareStream(PATCH_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_PATCH_FRAME_MAT, FIELD_PATCH_COUNT));
    }
}
