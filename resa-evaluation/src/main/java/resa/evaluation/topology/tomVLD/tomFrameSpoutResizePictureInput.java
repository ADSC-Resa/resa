package resa.evaluation.topology.tomVLD;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_imgproc;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_highgui.cvLoadImage;
import static resa.evaluation.topology.tomVLD.Constants.*;
import static resa.evaluation.topology.tomVLD.StormConfigManager.getInt;
import static resa.evaluation.topology.tomVLD.StormConfigManager.getString;


/**
 * Created by Intern04 on 4/8/2014.
 */
public class tomFrameSpoutResizePictureInput extends BaseRichSpout {
    SpoutOutputCollector collector;
    private int frameCount;
    private long lastFrameTime;
    private int delayInMS;

    int firstFrameId;
    int lastFrameId;
    int W,H;
    private String path;
    private String imageFolder;
    private String filePrefix;

    int targetCount = 0;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        W = ConfigUtil.getInt(map, "width", 640);
        H = ConfigUtil.getInt(map, "height", 480);
        this.collector = spoutOutputCollector;
        delayInMS = getInt(map, "inputFrameDelay");

        firstFrameId = getInt(map, "firstFrameId");
        lastFrameId = getInt(map, "lastFrameId");
        path = getString(map, "sourceFilePath");
        imageFolder = getString(map, "imageFolder");
        filePrefix = getString(map, "filePrefix", "frame");

        frameCount = 0;
        targetCount = lastFrameId - firstFrameId + 1;
    }

    @Override
    public void nextTuple() {
        long now = System.currentTimeMillis();
        if (now - lastFrameTime < delayInMS) {
            return;
        } else {
            lastFrameTime = now;
        }

        if (frameCount < targetCount) {
            try {
                long start = System.currentTimeMillis();
                String fileName = path + imageFolder + System.getProperty("file.separator")
                        + String.format("%s%06d.jpg", filePrefix, (frameCount + firstFrameId));
                File f = new File(fileName);

                if (!f.exists()) {
                    throw new  FileNotFoundException(fileName + " does not exist");
                }
                System.out.printf("Get file");
                opencv_core.IplImage imageFk = new opencv_core.IplImage();
                opencv_core.Mat mat = opencv_highgui.imread(fileName, opencv_highgui.CV_LOAD_IMAGE_COLOR);
                System.out.printf("Finish read file");
                opencv_core.Mat matNew = new opencv_core.Mat();
                opencv_core.Size size = new opencv_core.Size(W, H);
                opencv_imgproc.resize(mat, matNew, size);
                System.out.printf("Finish resize");
                Serializable.Mat sMat = new Serializable.Mat(matNew);

                collector.emit(RAW_FRAME_STREAM, new Values(frameCount, sMat), frameCount);
                frameCount++;
                long nowTime = System.currentTimeMillis();
                System.out.printf("Sendout: " + nowTime + "," + frameCount + ",used: " + (nowTime -start));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }


}
