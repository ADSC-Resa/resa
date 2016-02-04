package resa.evaluation.topology.tomVLD;

import backtype.storm.metric.SystemBolt;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;

import java.util.Map;

import static resa.evaluation.topology.tomVLD.Constants.*;
import static resa.evaluation.topology.tomVLD.StormConfigManager.getInt;
import static resa.evaluation.topology.tomVLD.StormConfigManager.getString;


/**
 * Created by Intern04 on 4/8/2014.
 */
public class tomFrameSpoutResizeFox extends BaseRichSpout {
    SpoutOutputCollector collector;
    private String SOURCE_FILE;
    private FFmpegFrameGrabber grabber;
    private int frameId;
    private long lastFrameTime;
    private int delayInMS;
    private long openTimeStamp;

    int firstFrameId;
    int lastFrameId;
    int endFrameID;
    int W,H;
    int sampleID;
    int sampleFrames;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        frameId = 0;
        sampleID = 0;
        sampleFrames = ConfigUtil.getInt(map, "sampleFrames", 1);

        firstFrameId = getInt(map, "firstFrameId");
        lastFrameId = getInt(map, "lastFrameId");
        SOURCE_FILE = getString(map, "videoSourceFile");
        grabber = new FFmpegFrameGrabber(SOURCE_FILE);
        System.out.println("Created capture: " + SOURCE_FILE);

        W = ConfigUtil.getInt(map, "width", 640);
        H = ConfigUtil.getInt(map, "height", 480);

        delayInMS = getInt(map, "inputFrameDelay");

        this.collector = spoutOutputCollector;
        try {
            grabber.start();
            while (++frameId < firstFrameId)
                grabber.grab();

        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        }

        //kp.deallocate();

        if (Debug.topologyDebugOutput)
            System.out.println("Grabber started");

        if (Debug.timer)
            System.out.println("TIME=" + System.currentTimeMillis());

        //TODO: caustion, for RedisStreamProducerBeta version, we reajust the start and end frameID!!!
        int diff = lastFrameId - firstFrameId + 1;
        frameId = 0;
        endFrameID = frameId + diff;
        openTimeStamp = System.currentTimeMillis();

    }

    opencv_core.IplImage image;
    opencv_core.Mat mat;

    @Override
    public void nextTuple() {
        long now = System.currentTimeMillis();
        if (now - lastFrameTime < delayInMS) {
            return;
        } else {
            lastFrameTime = now;
        }

        if (frameId < endFrameID) {
            try {
                long start = System.currentTimeMillis();
                image = grabber.grab();
                mat = new opencv_core.Mat(image);
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
                System.out.printf("Sendout: " + nowTime + "," + frameId + ",used: " + (nowTime -start));
                frameId++;

            } catch (FrameGrabber.Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        long failedTS = System.currentTimeMillis();
        long durationMins = (failedTS - openTimeStamp) / 60000;
        System.out.printf("FailedFrameID: " + msgId + ",at: " + failedTS + ", durationMin: " + durationMins);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
        outputFieldsDeclarer.declareStream(SAMPLE_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_SAMPLE_ID));
    }


}
