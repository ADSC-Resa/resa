package resa.evaluation.tools;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;
import redis.clients.jedis.Jedis;
import resa.evaluation.topology.tomVLD.Serializable;
import resa.util.ConfigUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.bytedeco.javacpp.opencv_highgui.cvLoadImage;
import static org.bytedeco.javacpp.opencv_highgui.cvSaveImage;
import static resa.evaluation.topology.tomVLD.StormConfigManager.getString;

/**
 * Created by ding on 14-3-18.
 */
public class ImageSenderWithLoop {

    private static final File END = new File("END");

    private String host;
    private int port;
    private byte[] queueName;
    private String path;
    private String imageFolder;
    private String filePrefix;

    public ImageSenderWithLoop(Map<String, Object> conf) {
        this.host = (String) conf.get("redis.host");
        this.port = ((Number) conf.get("redis.port")).intValue();
        this.queueName = ((String) conf.get("redis.queue")).getBytes();
        this.path = getString(conf, "sourceFilePath");
        this.imageFolder = getString(conf, "imageFolder");
        this.filePrefix = getString(conf, "filePrefix");
    }

    public void send2Queue(int st, int end, int fps, int range, int safeLen, int stopTime, int mode) throws IOException {
        Jedis jedis = new Jedis(host, port);
        int generatedFrames = 0;
        int fileIndex = st;

        try {
            long start = System.currentTimeMillis();
            long last = start;
            long qLen = 0;
            int target = fps + (int)((2 * Math.random() - 1) * range);
            int finished = 0;

            while (true) {

                String fileName = path + imageFolder + System.getProperty("file.separator")
                        + String.format("%s%06d.jpg", filePrefix, (fileIndex++));
                File f = new File(fileName);
                if (f.exists() == false) {
                    System.out.println("File not exist: " + fileName);
                    continue;
                }

                if (mode == 0) {
                    opencv_core.IplImage image = cvLoadImage(fileName);
                    opencv_core.Mat matOrg = new opencv_core.Mat(image);
                    Serializable.Mat sMat = new Serializable.Mat(matOrg);
                    jedis.rpush(this.queueName, sMat.toByteArray());
                } else {

                    jedis.rpush(this.queueName, Files.readAllBytes(new File(fileName).toPath()));
                }

                if (fileIndex > end){
                    fileIndex = st;
                }

                if (finished ++ == 0) {
                    generatedFrames ++;
                    long current = System.currentTimeMillis();
                    long elapse = current - last;
                    long remain = 1000 - elapse;
                    if (remain > 0) {
                        Thread.sleep(remain);
                    }
                    last = System.currentTimeMillis();
                    qLen = jedis.llen(this.queueName);
                    System.out.println("Target: " + target + ", elapsed: " + (last - start) / 1000
                            + ",totalSend: " + generatedFrames+ ", remain: " + remain + ", sendQLen: " + qLen);
                    while (qLen > safeLen){
                        Thread.sleep(Math.max(100, stopTime));
                        System.out.println("qLen > safeLen, stop sending....");
                        qLen = jedis.llen(this.queueName);
                    }
                    target = fps + (int)((2 * Math.random() - 1) * range);
                    finished = 0;
                }
            }

        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("usage: ImageSender <confFile> <fileSt> <fileEnd> <fps> <range> <safeLen> <stopTime> <mode>");
            return;
        }
        ImageSenderWithLoop sender = new ImageSenderWithLoop(ConfigUtil.readConfig(new File(args[0])));
        System.out.println(String.format("start sender, st: %d, end: %d, fps: %d, r: %d, slen: %d, sTime: %d, mode: %d",
                Integer.parseInt(args[1]), Integer.parseInt(args[2]),
                Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                Integer.parseInt(args[5]), Integer.parseInt(args[6]), Integer.parseInt(args[7])));
        sender.send2Queue(Integer.parseInt(args[1]), Integer.parseInt(args[2]),
                Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                Integer.parseInt(args[5]), Integer.parseInt(args[6]), Integer.parseInt(args[7]));
        System.out.println("end sender");
    }

}
