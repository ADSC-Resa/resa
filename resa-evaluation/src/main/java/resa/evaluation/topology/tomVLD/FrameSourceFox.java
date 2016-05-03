package resa.evaluation.topology.tomVLD;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import resa.topology.RedisQueueSpout;

import java.util.Map;

import static resa.evaluation.topology.tomVLD.Constants.*;

/**
 * Updated on Aug 5,  the way to handle frame sampling issue is changed, this is pre-processed by the spout not to
 * send out unsampled frames to the patch generation bolt.
 * also binding to ImageSenderFox!
 */
public class FrameSourceFox extends RedisQueueSpout {

    private int frameId;
    private int sampleID;
    private int sampleFrames;

    int W,H;

    public FrameSourceFox(String host, int port, String queue) {
        super(host, port, queue, true);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
        frameId = 0;
        sampleID = 0;
        sampleFrames = ConfigUtil.getInt(conf, "sampleFrames", 1);

        W = ConfigUtil.getInt(conf, "width", 640);
        H = ConfigUtil.getInt(conf, "height", 480);
    }

    @Override
    protected void emitData(Object data) {
        String id = String.valueOf(frameId);
        byte[] imgBytes = (byte[]) data;

        opencv_core.IplImage fkImage = new opencv_core.IplImage();
        Serializable.Mat revMat = new Serializable.Mat(imgBytes);

        opencv_core.Mat mat = revMat.toJavaCVMat();
        opencv_core.Mat matNew = new opencv_core.Mat();
        opencv_core.Size size = new opencv_core.Size(W, H);
        opencv_imgproc.resize(mat, matNew, size);

        Serializable.Mat sMat = new Serializable.Mat(matNew);

        collector.emit(RAW_FRAME_STREAM, new Values(frameId, sMat), frameId);
        if (frameId % sampleFrames == 0) {
            collector.emit(SAMPLE_FRAME_STREAM, new Values(frameId, sMat, sampleID), frameId);
            sampleID ++;
        }
        long nowTime = System.currentTimeMillis();
        System.out.printf("Sendout: " + nowTime + "," + frameId);
        frameId ++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
        declarer.declareStream(SAMPLE_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_SAMPLE_ID));
    }
}
