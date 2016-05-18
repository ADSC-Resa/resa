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

    private BlockingQueue<Serializable.Mat> dataQueue = new ArrayBlockingQueue<>(10000);

    public ImageSenderWithLoop(Map<String, Object> conf) {
        this.host = (String) conf.get("redis.host");
        this.port = ((Number) conf.get("redis.port")).intValue();
        this.queueName = ((String) conf.get("redis.queue")).getBytes();
        this.path = getString(conf, "sourceFilePath");
        this.imageFolder = getString(conf, "imageFolder");
        this.filePrefix = getString(conf, "filePrefix");
    }

    public void send2Queue(int st, int end, int fps, int retain) throws IOException {
        int generatedFrames = st;
        opencv_core.IplImage fk = new opencv_core.IplImage();
        List<Integer> array = new ArrayList(IntStream.range(0, fps).boxed().collect(Collectors.toList()));
        for (int i = 0; i < 3; i++) {
            new PushThread().start();
        }
        Random rand = new Random();
        int range = Math.min(fps - retain, retain);
        try {
            long now;
            Set<Integer> retainFrames = new HashSet<>();
            while (true) {
                Collections.shuffle(array);
                int count = retain + (int) ((2 * rand.nextDouble() - 1) * range);
                retainFrames.clear();
                for (int j = 0; j < count; j++) {
                    retainFrames.add(array.get(j));
                }
                now = System.currentTimeMillis();
                System.out.println(count + "@" + now + ", qLen=" + dataQueue.size());
                for (int j = 0; j < fps; j++) {
                    if (retainFrames.contains(j)) {
                        String fileName = path + imageFolder + System.getProperty("file.separator")
                                + String.format("%s%06d.jpg", filePrefix, (++generatedFrames));
                        opencv_core.IplImage image = cvLoadImage(fileName);
                        opencv_core.Mat matOrg = new opencv_core.Mat(image);
                        Serializable.Mat sMat = new Serializable.Mat(matOrg);

                        dataQueue.put(sMat);
                        if (generatedFrames > end) {
                            generatedFrames = st;
                        }
                    }
                }
                long sleep = now + 1000 - System.currentTimeMillis();
                if (sleep > 0) {
                    Thread.sleep(sleep);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class PushThread extends Thread {

        private Jedis jedis = new Jedis(host, port);

        private PushThread() {
        }

        @Override
        public void run() {
            Serializable.Mat sMat;
            try {
                while ((sMat = dataQueue.take()) != null) {
                    jedis.rpush(queueName, sMat.toByteArray());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("usage: ImageSender <confFile> <fileSt> <fileEnd> <fps> <retain>");
            return;
        }
        ImageSenderWithLoop sender = new ImageSenderWithLoop(ConfigUtil.readConfig(new File(args[0])));
        System.out.println("start sender");
        sender.send2Queue(Integer.parseInt(args[1]), Integer.parseInt(args[2]),
                Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        System.out.println("end sender");
    }

}
