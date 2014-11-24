package resa.evaluation.tools;

import redis.clients.jedis.Jedis;
import resa.util.ConfigUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by ding on 14-6-17.
 */
public class TweetsSimulator {

    private static final String END = new String("end");

    private String host;
    private int port;
    private String queueName;
    private double speedRatio;
    private long counter = 0;
    private BlockingQueue<String> dataQueue = new ArrayBlockingQueue<>(10000);

    public TweetsSimulator(Map<String, Object> conf, double speedRatio) {
        this.host = (String) conf.get("redis.host");
        this.port = ((Number) conf.get("redis.port")).intValue();
        this.queueName = (String) conf.get("redis.queue");
        this.speedRatio = speedRatio;
    }

    public TweetsSimulator(Map<String, Object> conf) {
        this(conf, 1);
    }

    public void setSpeedRatio(double speedRatio) {
        this.speedRatio = speedRatio;
    }

    private class PushThread extends Thread {

        private Jedis jedis = new Jedis(host, port);

        @Override
        public void run() {
            String line = null;
            try {
                while ((line = dataQueue.take()) != END) {
                    jedis.rpush(queueName, line);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                dataQueue.offer(END);
            }
        }
    }

    public void start(Path input) {
        for (int i = 0; i < 3; i++) {
            new PushThread().start();
        }
        if (Files.isDirectory(input)) {
            try {
                Files.list(input).forEach(f -> start0(f));
            } catch (IOException e) {
            }
        } else {
            start0(input);
        }
        try {
            dataQueue.put(END);
        } catch (InterruptedException e) {
        }
    }

    private static long currTimeSecs() {
        return System.currentTimeMillis() / 1000;
    }

    private long parseTimeSecs(String time) {
        try {
            return (long) (Long.parseLong(time) / speedRatio);
        } catch (Exception e) {
            return -1;
        }
    }

    private void start0(Path inputFile) {
        long start = currTimeSecs();
        long firstSec = 0;
        String line = null;
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            while ((line = reader.readLine()) != null) {
                int split = line.indexOf('@');
                long sec;
                String data;
                if (split < 0 || (sec = parseTimeSecs(line.substring(0, split))) < 0) {
                    data = line;
                } else {
                    if (firstSec == 0) {
                        firstSec = sec;
                    } else {
                        while (sec - firstSec > currTimeSecs() - start) {
                            Thread.sleep(5);
                        }
                    }
                    data = line.substring(split + 1);
                }
                dataQueue.put(data);
                counter++;
                if (counter % 10000 == 0) {
                    System.out.println("Finished " + counter + " lines");
                }
            }
        } catch (Exception e) {
            System.out.println(line);
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("usage: DataSender <confFile> <inputFile> [speedRatio]");
            return;
        }
        TweetsSimulator simulator = new TweetsSimulator(ConfigUtil.readConfig(new File(args[0])));
        if (args.length == 3) {
            simulator.setSpeedRatio(Double.parseDouble(args[2]));
        }
        System.out.println("start simulator");
        Path dataFile = Paths.get(args[1]);
        simulator.start(dataFile);
        System.out.println("end simulator");
    }

}
