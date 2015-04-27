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
public class tPatchGeneraterGamma extends BaseRichBolt {
    OutputCollector collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        opencv_core.IplImage fk = new opencv_core.IplImage();

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.PatchIdentifierMat patchIdentifierMat = (Serializable.PatchIdentifierMat) tuple.getValueByField(FIELD_PATCH_FRAME_MAT);
        int[] info = (int[]) tuple.getValueByField(FIELD_PATCH_COUNT);

        //int[] info = new int[]{w, W, dx, h, H, dy, totalPatchCount};
        int w = info[0];
        int W = info[1];
        int dx = info[2];
        int h = info[3];
        int H = info[4];
        int dy = info[5];
        int totalPatchCount = info[6];
        Serializable.Mat sMat = patchIdentifierMat.sMat;
        Serializable.PatchIdentifier identifier = patchIdentifierMat.identifier;
        opencv_core.Mat mat = sMat.toJavaCVMat();
        int y = identifier.roi.y;

        for (int x = 0; x + w <= W; x += dx) {

            Serializable.Rect rect = new Serializable.Rect(x, 0, w, h);
            opencv_core.Mat pMat = new opencv_core.Mat(mat, rect.toJavaCVRect());
            Serializable.Mat pSMat = new Serializable.Mat(pMat);

            Serializable.Rect adjRect = new Serializable.Rect(x, y, w, h);
            Serializable.PatchIdentifierMat subPatchMat = new Serializable.PatchIdentifierMat(frameId, adjRect, pSMat);

            collector.emit(PATCH_FRAME_STREAM, tuple, new Values(frameId, subPatchMat, totalPatchCount));
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PATCH_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_PATCH_FRAME_MAT, FIELD_PATCH_COUNT));
    }
}
