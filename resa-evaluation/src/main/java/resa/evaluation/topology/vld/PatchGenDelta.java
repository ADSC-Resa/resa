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
import resa.evaluation.topology.tomVLD.Serializable;

import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static resa.evaluation.topology.vld.Constant.*;

/**
 * Created by ding on 14-7-3.
 * This beta version is Modified by Tom Fu, on April 2016
 * We mainly re-design the topology to remove those broadcasting issue (all grouping), here for experimental purpose
 */
public class PatchGenDelta extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
//        byte[] imgBytes = (byte[]) input.getValueByField(FIELD_IMG_BYTES);
//        IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));
//        opencv_core.Mat matOrg = new opencv_core.Mat(image);
//        Serializable.Mat sMat = new Serializable.Mat(matOrg);
        int frameId = input.getIntegerByField(FIELD_FRAME_ID);
//        opencv_core.IplImage fk = new opencv_core.IplImage();
        byte[] imgBytes = (byte[]) input.getValueByField(FIELD_IMG_BYTES);
        Serializable.Mat sMat = new Serializable.Mat(imgBytes);

        double fx = .25, fy = .25;
//        double fsx = .5, fsy = .5;
        double fsx = 1.0, fsy = 1.0;

        int W = sMat.getCols(), H = sMat.getRows();
        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);
        int patchCount = 0;
        for (int x = 0; x + w <= W; x += dx)
            for (int y = 0; y + h <= H; y += dy)
                patchCount++;

        for (int x = 0; x + w <= W; x += dx) {
            for (int y = 0; y + h <= H; y += dy) {
                Serializable.Rect rect = new Serializable.Rect(x, y, w, h);

                opencv_core.Mat pMat = new opencv_core.Mat(sMat.toJavaCVMat(), rect.toJavaCVRect());
                Serializable.Mat pSMat = new Serializable.Mat(pMat);
                Serializable.PatchIdentifierMat subPatchMat = new Serializable.PatchIdentifierMat(frameId, rect, pSMat);

                collector.emit(PATCH_FRAME_STREAM, input, new Values(frameId, subPatchMat, patchCount));
            }
        }

//        try {
//            cvReleaseImage(image);
//        } catch (Exception e) {
//        }

        System.out.println("FrameID: " + frameId + ", patchCount: " + patchCount);
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(PATCH_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_PATCH_FRAME_MAT, FIELD_PATCH_COUNT));
    }
}
