package resa.evaluation.topology.tomVLD;

import org.bytedeco.javacpp.opencv_core;
import redis.clients.jedis.Jedis;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.util.PriorityQueue;

/**
 * Created by Tom Fu
 * RedisStreamProducerBeta keeps a timer for each frame, if the expected frame is late, it starts the time and wait until timeout,
 * then it simply drops this frame and come to the next expected frame. (suitable for loss insensitive application)
 */
public class RedisStreamProducerBeta implements Runnable {
    /**
     * Ordered queue for putting frames in order
     */
    private PriorityQueue<StreamFrame> stream;
    /**
     * Has the last expected frame come?
     */
    private boolean finished;

    //private static final byte[] END = new String("END").getBytes();
    private String host;
    private int port;
    private byte[] queueName;
    //private BlockingQueue<byte[]> dataQueue = new ArrayBlockingQueue<>(10000);
    private Jedis jedis;

    private long sleepTime;
    private int startFrameID;
    private int maxWaitCount;

    /**
     * Creates a producer expecting frames in range [firstFrameId, lastFrameId)
     */
    public RedisStreamProducerBeta(String host, int port, String queueName) {

        stream = new PriorityQueue<>();
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
        finished = false;
        jedis = new Jedis(host, port);
        this.maxWaitCount = 4;
        this.startFrameID = 1;
        this.sleepTime = 10;
    }

    /**
     * Creates a producer expecting frames in range [firstFrameId, lastFrameId), with an additional parameter qSize
     */
    public RedisStreamProducerBeta(String host, int port, String queueName,
                                   int startFrameID, int maxWaitCount, int sleepTime) {

        stream = new PriorityQueue<>();
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
        finished = false;
        jedis = new Jedis(host, port);
        this.startFrameID = startFrameID;
        this.maxWaitCount = maxWaitCount;
        this.sleepTime = sleepTime;

        System.out.println("Check_init_RedisStreamProducerBeta, " + System.currentTimeMillis() +
                ", host: " + this.host + ", port: " + this.port + ", qName: " + this.queueName +
                ", stFrameID: " + this.startFrameID + ", sleepTime: " + this.sleepTime + ", mWaitCnt: " + this.maxWaitCount);
    }

    /**
     * Add frame to the queue if it is fully processed
     */
    public void addFrame(StreamFrame streamFrame) {
        synchronized (stream) {
            stream.add(streamFrame);
        }
    }

    /**
     * Get expected frame from the queue.
     *
     * @return next expected frame, or null if it has not come yet.
     */
    public StreamFrame pollFrame() {
        synchronized (stream) {
            return stream.poll();
        }
    }

    public StreamFrame getPeekFrame() {
        synchronized (stream) {
            return stream.isEmpty() ? null : stream.peek();
        }
    }

    public int getStreamSize() {
        synchronized (stream) {
            return stream.size();
        }
    }

    @Override
    public void run() {
        int currentFrameID = startFrameID;
        int waitCount = 0;
        while (!finished) {
            try {
                StreamFrame peekFrame = getPeekFrame();
                if (peekFrame == null) {
                    //System.out.println("peekFrame == null");
                    Thread.sleep(sleepTime);
                } else {
                    if (peekFrame.frameId <= currentFrameID) {
                        //System.out.println("peekFrame.frameId (" + peekFrame.frameId +") <= currentFrameID: " + currentFrameID);
                        pollFrame();

                    } else if (peekFrame.frameId == (currentFrameID + 1)) {
                        //System.out.println("peekFrame.frameId (" + peekFrame.frameId +") == 1 + currentFrameID: " + currentFrameID);
                        StreamFrame nextFrame = pollFrame();
                        opencv_core.IplImage iplImage = nextFrame.image.asIplImage();
                        BufferedImage bufferedImage = iplImage.getBufferedImage();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        ImageIO.write(bufferedImage, "JPEG", baos);
                        jedis.rpush(this.queueName, baos.toByteArray());

                        System.out.println("finishedAdd: " + System.currentTimeMillis() + ",Fid: " + nextFrame.frameId);
                        currentFrameID++;
                    } else {
                        //System.out.println("peekFrame.frameId (" + peekFrame.frameId +") >> currentFrameID: " + currentFrameID);
                        Thread.sleep(sleepTime);
                        waitCount++;
                        if (waitCount >= maxWaitCount) {
                            System.out.println("frameTimeout, traceID: " + currentFrameID + ", peek: " + peekFrame.frameId + ", qSize: " + stream.size());
                            currentFrameID++;
                            waitCount = 0;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.print("Exception: ");
                e.printStackTrace();
            }
        }
    }
}
