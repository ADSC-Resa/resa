package resa.evaluation.topology.vld;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core.*;
import org.bytedeco.javacpp.opencv_features2d.KeyPoint;
import org.bytedeco.javacpp.opencv_nonfree.SIFT;
import resa.evaluation.topology.tomVLD.Serializable;
import resa.util.ConfigUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static resa.evaluation.topology.vld.Constant.*;

/**
 * Created by ding on 14-7-3.
 * This beta version is Modified by Tom Fu, on April 2016
 * We mainly re-design the topology to remove those broadcasting issue (all grouping), here for experimental purpose
 */
public class FeatureExtracterDelta extends BaseRichBolt {

    private SIFT sift;
    private double[] buf;
    private OutputCollector collector;
    private boolean showDetails;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        int nfeatures = ConfigUtil.getInt(stormConf, "sift-nfeatures", 0);
        double contrastThreshold = ConfigUtil.getDouble(stormConf, "sift-contrastThreshold", 0.04);
        int edgeThreshold = ConfigUtil.getInt(stormConf, "sift-edgeThreshold", 10);
        sift = new SIFT(nfeatures, 3, contrastThreshold, edgeThreshold, 1.6);
        buf = new double[128];
        this.collector = collector;

        showDetails = ConfigUtil.getBoolean(stormConf, "show-details", false);

        System.out.println("FeatureExtracterDelta.prepare, nfeatures: " + nfeatures + ", contrastThreshold: "
                + contrastThreshold + ", edgeThreshold: " + edgeThreshold);
    }

    @Override
    public void execute(Tuple input) {
        int frameId = input.getIntegerByField(FIELD_FRAME_ID);
        Serializable.PatchIdentifierMat identifierMat = (Serializable.PatchIdentifierMat) input.getValueByField(FIELD_PATCH_FRAME_MAT);
        int patchCount = input.getIntegerByField(FIELD_PATCH_COUNT);

        KeyPoint points = new KeyPoint();
        Mat featureDesc = new Mat();
        Mat matImg = identifierMat.sMat.toJavaCVMat();
        sift.detect(matImg, points);
        sift.compute(matImg, points, featureDesc);

        int rows = featureDesc.rows();
        List<byte[]> selected = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            featureDesc.rows(i).asCvMat().get(buf);
            // compress data
            byte[] siftFeat = new byte[buf.length];
            for (int j = 0; j < buf.length; j++) {
                siftFeat[j] = (byte) (((int) buf[j]) & 0xFF);
            }
            selected.add(siftFeat);
        }
        if (showDetails) {
            System.out.println("FrameID: " + frameId + ", rows: " + rows);
        }
        collector.emit(STREAM_FEATURE_DESC, input, new Values(frameId, selected, patchCount));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_FEATURE_DESC, new Fields(FIELD_FRAME_ID, FIELD_FEATURE_DESC, FIELD_PATCH_COUNT));
    }
}
