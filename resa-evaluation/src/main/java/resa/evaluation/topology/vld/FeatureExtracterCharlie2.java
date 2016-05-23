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
import resa.util.ConfigUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static resa.evaluation.topology.vld.Constant.*;

/**
 * Created by ding on 14-7-3.
 * This beta version is Modified by Tom Fu, on April 2016
 * We mainly re-design the topology to remove those broadcasting issue (all grouping), here for experimental purpose
 */
public class FeatureExtracterCharlie2 extends BaseRichBolt {

    private SIFT sift;
    private double[] buf;
    private OutputCollector collector;
    private int groupNumber;
    private int minGroupSize;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        int nfeatures = ConfigUtil.getInt(stormConf, "sift-nfeatures", 0);
        double contrastThreshold = ConfigUtil.getDouble(stormConf, "sift-contrastThreshold", 0.04);
        int edgeThreshold = ConfigUtil.getInt(stormConf, "sift-edgeThreshold", 10);
        sift = new SIFT(nfeatures, 3, contrastThreshold, edgeThreshold, 1.6);
        buf = new double[128];
        this.collector = collector;
        minGroupSize = ConfigUtil.getInt(stormConf, "vd-minGroupSize", 50);
        groupNumber = ConfigUtil.getInt(stormConf, "vd-group.count", 1);

        System.out.println("FeatureExtracterCharlie2.prepare, nfeatures: " + nfeatures + ", contrastThreshold: "
                + contrastThreshold + ", edgeThreshold: " + edgeThreshold + ",minGroupSize: " + minGroupSize + ", groupNumber: " + groupNumber);
    }

    @Override
    public void execute(Tuple input) {
        byte[] imgBytes = (byte[]) input.getValueByField(FIELD_IMG_BYTES);
        IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));
        String frameId = input.getStringByField(FIELD_FRAME_ID);

        KeyPoint points = new KeyPoint();
        Mat featureDesc = new Mat();
        Mat matImg = new Mat(image);
        sift.detect(matImg, points);
        sift.compute(matImg, points, featureDesc);
        try {
            cvReleaseImage(image);
        } catch (Exception e) {
        }
        int rows = featureDesc.rows();
        int tempGroupNumber = groupNumber;

        if (rows > 0) {
            if (rows < tempGroupNumber * minGroupSize) {
                tempGroupNumber = (rows - 1) / minGroupSize + 1;
            }

            List<List<byte[]>> toSend = new ArrayList<>();
            for (int i = 0; i < tempGroupNumber; i ++){
                List<byte[]> selected = new ArrayList<>();
                toSend.add(selected);
            }
            for (int i = 0; i < rows; i++) {
                featureDesc.rows(i).asCvMat().get(buf);
                // compress data
                byte[] siftFeat = new byte[buf.length];
                for (int j = 0; j < buf.length; j++) {
                    siftFeat[j] = (byte) (((int) buf[j]) & 0xFF);
                }
                int tIndex = i % tempGroupNumber;
                toSend.get(tIndex).add(siftFeat);
            }

            for (int i = 0; i < toSend.size(); i++) {
                collector.emit(STREAM_FEATURE_DESC, input, new Values(frameId, toSend.get(i), rows, tempGroupNumber));
            }

        } else {
            ///rows == 0
            List<byte[]> emtpy = new ArrayList<>();
            collector.emit(STREAM_FEATURE_DESC, input, new Values(frameId, emtpy, 0, 1));
        }

        System.out.println("FrameID: " + frameId + ", rows: " + rows + ", tempGroupNum: " + tempGroupNumber);
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_FEATURE_DESC, new Fields(FIELD_FRAME_ID, FIELD_FEATURE_DESC, FIELD_FEATURE_CNT, SEND_GROUP_CNT));
    }
}
