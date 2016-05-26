package resa.evaluation.topology.vld;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.*;
import org.bytedeco.javacpp.opencv_features2d.KeyPoint;
import org.bytedeco.javacpp.opencv_nonfree.SIFT;
import resa.util.ConfigUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static resa.evaluation.topology.vld.Constant.*;

/**
 * Created by ding on 14-7-3.
 * This beta version is Modified by Tom Fu, on April 2016
 * We mainly re-design the topology to remove those broadcasting issue (all grouping), here for experimental purpose
 */
public class FeatureExtracterDelta2 extends BaseRichBolt {

    private SIFT sift;
    private double[] buf;
    private OutputCollector collector;
    private boolean showDetails;
    double fx, fy, fsx, fsy;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        int nfeatures = ConfigUtil.getInt(stormConf, "sift-nfeatures", 0);
        double contrastThreshold = ConfigUtil.getDouble(stormConf, "sift-contrastThreshold", 0.04);
        int edgeThreshold = ConfigUtil.getInt(stormConf, "sift-edgeThreshold", 10);
        sift = new SIFT(nfeatures, 3, contrastThreshold, edgeThreshold, 1.6);
        buf = new double[128];
        this.collector = collector;
        fx = ConfigUtil.getDouble(stormConf, "fx", 0.25);
        fy = ConfigUtil.getDouble(stormConf, "fy", 0.25);
        fsx = ConfigUtil.getDouble(stormConf, "fsx", 0.5);
        fsy = ConfigUtil.getDouble(stormConf, "fsy", 0.5);

        showDetails = ConfigUtil.getBoolean(stormConf, "show-details", false);

        System.out.println("FeatureExtracterDelta2.prepare, nfeatures: " + nfeatures + ", contrastThreshold: "
                + contrastThreshold + ", edgeThreshold: " + edgeThreshold);
    }

    @Override
    public void execute(Tuple input) {
        byte[] imgBytes = (byte[]) input.getValueByField(FIELD_IMG_BYTES);
        IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));
        String frameId = input.getStringByField(FIELD_FRAME_ID);
        Mat matImg = new Mat(image);

        int W = matImg.cols(), H = matImg.rows();
        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);
        int patchCount = 0;
        int xCnt = 0;
        for (int x = 0; x + w <= W; x += dx) {
            xCnt++;
            for (int y = 0; y + h <= H; y += dy) {
                patchCount++;
            }
        }

        List<List<byte[]>> toSend = new ArrayList<>(patchCount);
        List<Integer> array = new ArrayList(IntStream.range(0, patchCount).boxed().collect(Collectors.toList()));
        Collections.shuffle(array);

        int rowSum = 0;
        for (int i = 0; i < array.size(); i++) {
            int y = array.get(i) / xCnt * dy;
            int x = array.get(i) % xCnt * dx;

            Rect rect = new Rect(x, y, w, h);
            opencv_core.Mat pMat = new opencv_core.Mat(matImg, rect);

            KeyPoint points = new KeyPoint();
            Mat featureDesc = new Mat();
            sift.detect(pMat, points);
            sift.compute(pMat, points, featureDesc);

            int rows = featureDesc.rows();
            List<byte[]> selected = new ArrayList<>(rows);

            for (int r = 0; r < rows; r++) {
                featureDesc.rows(r).asCvMat().get(buf);
                // compress data
                byte[] siftFeat = new byte[buf.length];
                for (int j = 0; j < buf.length; j++) {
                    siftFeat[j] = (byte) (((int) buf[j]) & 0xFF);
                }
                selected.add(siftFeat);
            }
            toSend.add(selected);

            if (showDetails){
                System.out.println("FrameID: " + frameId + ", i: " + i + ", rows: " + rows);
                rowSum += rows;
            }
        }

        for (int i = 0; i < toSend.size(); i++) {
            collector.emit(STREAM_FEATURE_DESC, input, new Values(frameId, toSend.get(i), patchCount));
        }

        if (showDetails) {
            System.out.println("FrameID: " + frameId + ", patchCount: " + toSend.size() + ", rowSum: " + rowSum);
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_FEATURE_DESC, new Fields(FIELD_FRAME_ID, FIELD_FEATURE_DESC, FIELD_PATCH_COUNT));
    }
}
