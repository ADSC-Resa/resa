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
 * Created by Tom FU.
 *
 * Be careful about the different W*H of the video and camera input
 */
public class tVLDTransBolt extends BaseRichBolt {
    OutputCollector collector;

    //List<Integer> targetComponentTasks;
    //String targetComponentName;
    int w, h, dx, dy, W, H, totalPatchCount;
    double fx, fy, fsx, fsy;
    private int sampleFrames;

//    public tVLDTransBolt(String targetComponentName){
//        this.targetComponentName = targetComponentName;
//    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        //opencv_core.IplImage fk = new opencv_core.IplImage();

        //TODO get params from config map
        fx = .25;
        fy = .25;
        fsx = .5;
        fsy = .5;
        //double fsx = .4, fsy = .4;
        W = ConfigUtil.getInt(map, "width", 728);
        H = ConfigUtil.getInt(map, "height", 408);
        w = (int) (W * fx + .5);
        h = (int) (H * fy + .5);
        dx = (int) (w * fsx + .5);
        dy = (int) (h * fsy + .5);

        int xCnt = 0;
        int yCnt = 0;
        for (int x = 0; x + w <= W; x += dx) {
            xCnt++;
        }
        for (int y = 0; y + h <= H; y += dy) {
            yCnt++;
        }

        totalPatchCount = xCnt * yCnt;
        sampleFrames = ConfigUtil.getInt(map, "sampleFrames", 1);
        System.out.println("tVLDTransBolt.prepare, W: " + W + ", H: " + H + ", w: " + w + ", h: " + h
                + ", dx: " + dx + ", dy: " + dy + ", totalCnt: " + totalPatchCount + ", sampleFrames: " + sampleFrames);
    }

    @Override
    public void execute(Tuple tuple) {

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        opencv_core.IplImage fk = new opencv_core.IplImage();

        collector.emit(RAW_FRAME_STREAM, tuple, new Values(frameId, sMat));

        if (frameId % sampleFrames == 0) {
            for (int y = 0; y + h <= H; y += dy) {
                Serializable.Rect rect = new Serializable.Rect(0, y, W, h);
                opencv_core.Mat pMat = new opencv_core.Mat(sMat.toJavaCVMat(), rect.toJavaCVRect());
                Serializable.Mat pSMat = new Serializable.Mat(pMat);
                Serializable.PatchIdentifierMat identifierMat = new Serializable.PatchIdentifierMat(frameId, rect, pSMat);
                int[] info = new int[]{w, W, dx, h, H, dy, totalPatchCount};
                collector.emit(PATCH_FRAME_STREAM, tuple, new Values(frameId, identifierMat, info));
                //System.out.println("send out, y: " + y);
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(PATCH_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_PATCH_FRAME_MAT, FIELD_PATCH_COUNT));
        outputFieldsDeclarer.declareStream(RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}
