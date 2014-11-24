package resa.migrate;

import backtype.storm.Config;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.utils.Utils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import resa.metrics.MetricNames;
import resa.topology.DelegatedBolt;
import resa.util.ConfigUtil;
import resa.util.NetUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

/**
 * Created by ding on 14-6-7.
 */
public class HdfsWritableBolt extends DelegatedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsWritableBolt.class);
    private static final Map<Integer, Object> DATA_HOLDER = new ConcurrentHashMap<>();
    // this should be part of user resource, but no initialization hook in work.clj yet
    private static CuratorFramework zk = null;

    private static CuratorFramework zkInstance(Map<String, Object> conf) {
        if (zk == null) {
            synchronized (HdfsWritableBolt.class) {
                if (zk == null) {
                    zk = Utils.newCuratorStarted(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                            conf.get(Config.STORM_ZOOKEEPER_PORT));
                }
            }
        }
        return zk;
    }


    private transient Map<String, Object> conf;
    private transient Kryo kryo;
    private transient Map<String, Object> dataRef;
    private Path loaclDataPath;
    private transient ExecutorService threadPool;
    private int myTaskId;
    private String taskZkNode;
    private Configuration hdfsConf;

    public HdfsWritableBolt(IRichBolt delegate) {
        super(delegate);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        this.conf = conf;
        this.hdfsConf = new Configuration();
        this.myTaskId = context.getThisTaskId();
        this.taskZkNode = String.format("%s/%s/task-%03d", conf.getOrDefault("resa.migrate.zkroot", "/resa"),
                context.getStormId(), context.getThisTaskId());
        ensureZkNode();
        kryo = SerializationFactory.getKryo(conf);
        //LOG.info("Bolt " + getDelegate().getClass() + " need persist");
        dataRef = getDataRef(context);
        loaclDataPath = Paths.get((String) conf.get(Config.STORM_LOCAL_DIR), "data", context.getStormId());
        if (!Files.exists(loaclDataPath)) {
            try {
                Files.createDirectories(loaclDataPath);
            } catch (FileAlreadyExistsException e) {
            } catch (IOException e) {
                LOG.warn("Cannot create data path: " + loaclDataPath, e);
                loaclDataPath = null;
            }
        }
        if (loaclDataPath != null) {
            loaclDataPath = loaclDataPath.resolve(String.format("task-%03d.data", context.getThisTaskId()));
        }
        long t1 = System.currentTimeMillis();
        seekAndLoadTaskData(context.getStormId(), context.getThisTaskId(), conf);
        long cost = System.currentTimeMillis() - t1;
        LOG.info("Task-{} Load data cost {}ms", context.getThisTaskId(), cost);
        this.threadPool = context.getSharedExecutor();
//        threadPool.submit(() -> writeTimeToRedis(context, cost));
        int checkpointInt = ConfigUtil.getInt(conf, "resa.comp.checkpoint.interval.sec", 180);
        if (checkpointInt > 0) {
            context.registerMetric(MetricNames.SERIALIZED_SIZE, () -> createCheckpointAndGetSize(context),
                    checkpointInt);
        }
        super.prepare(conf, context, outputCollector);
        if (!dataRef.isEmpty()) {
            reportMyData();
        }
    }

    private void writeTimeToRedis(TopologyContext context, long time) {
        Jedis jedis = new Jedis((String) conf.get("redis.host"), ConfigUtil.getInt(conf, "redis.port", 6379));
        jedis.set(context.getStormId() + "-" + context.getThisComponentId() + "-" + context.getThisTaskId()
                + "-start-time", time < 100 ? "zero" : String.valueOf(time));
        jedis.disconnect();
    }

    private void ensureZkNode() {
        try {
            if (zkInstance(conf).checkExists().forPath(taskZkNode) == null) {
                zkInstance(conf).create().creatingParentsIfNeeded().forPath(taskZkNode, "".getBytes());
            }
        } catch (Exception e) {
            throw new RuntimeException("Check task zkNode failed: " + taskZkNode, e);
        }
    }

    private String getDataLocation() {
        try {
            byte[] data = zkInstance(conf).getData().forPath(taskZkNode);
            String location = new String(data);
            return location.equals("") ? null : location;
        } catch (Exception e) {
            LOG.warn("Read host failed, taskZkNode=" + taskZkNode, e);
        }
        return null;
    }

    private void seekAndLoadTaskData(String topoId, int taskId, Map<String, Object> conf) {
        /* load data:
           1. find in memory, maybe data migration was not required
           2. retrieve data from src worker using TCP
           3. try to load checkpoint from redis or other distributed systems
        */
        Object data = DATA_HOLDER.remove(taskId);
        if (data != null) {
            dataRef.putAll((Map<String, Object>) data);
            LOG.info("Task-{} load data from memory", taskId);
            return;
        }
        try {
            FileSystem fs = FileSystem.get(hdfsConf);
            org.apache.hadoop.fs.Path file = getFilePath(conf, topoId, taskId);
            if (fs.exists(file)) {
                FSDataInputStream in = fs.open(file);
                loadData(in);
                in.close();
            }
        } catch (IOException e) {
            LOG.warn("read from hdfs failed for task-" + taskId, e);
        }
    }

    private org.apache.hadoop.fs.Path getFilePath(Map<String, Object> conf, String topoId, int taskId) {
        String topoName = topoId.split("-")[0];
        return new org.apache.hadoop.fs.Path(String.format("/resa/%s/task-%03d.data", topoName, taskId));
    }

    private static Map<String, Object> getDataRef(TopologyContext context) {
        try {
            Field f = context.getClass().getDeclaredField("_taskData");
            f.setAccessible(true);
            return (Map<String, Object>) f.get(context);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot get task data ref", e);
        }
    }

    private Long createCheckpointAndGetSize(TopologyContext context) {
        Long size = -1L;
        if (!dataRef.isEmpty() && loaclDataPath != null) {
            try {
                Path tmpFile = Files.createTempFile("resa-cp-", ".tmp");
                size = writeData(Files.newOutputStream(tmpFile));
                Files.move(tmpFile, loaclDataPath, StandardCopyOption.REPLACE_EXISTING);
                reportMyData();
                threadPool.submit(() -> {
                    org.apache.hadoop.fs.Path file = getFilePath(conf, context.getStormId(), context.getThisTaskId());
                    try (OutputStream out = FileSystem.get(hdfsConf).create(file, true, 12 * 1024, (short) 2,
                            32 * 1024 * 1024L)) {
                        Files.copy(loaclDataPath, out);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                LOG.warn("Save bolt failed", e);
            }
        }
        return size;
    }

    private void reportMyData() {
        try {
            zkInstance(conf).setData().forPath(taskZkNode, NetUtil.getLocalIP().getBytes());
        } catch (Exception e) {
            LOG.warn("Report my data failed", e);
        }
    }

    private int loadData(InputStream in) {
        Input kryoIn = new Input(in);
        int size = kryoIn.readInt();
        for (int i = 0; i < size; i++) {
            String key = kryoIn.readString();
            Class c = kryo.readClass(kryoIn).getType();
            Object v;
            if (KryoSerializable.class.isAssignableFrom(c)) {
                v = new DefaultSerializers.KryoSerializableSerializer().read(kryo, kryoIn, c);
            } else {
                v = kryo.readClassAndObject(kryoIn);
            }
            dataRef.put(key, v);
        }
        kryoIn.close();
        return kryoIn.total();
    }

    private long writeData(OutputStream out) {
        Output kryoOut = new Output(out);
        kryoOut.writeInt(dataRef.size());
        dataRef.forEach((k, v) -> {
            kryoOut.writeString(k);
            if (v instanceof KryoSerializable) {
                kryo.writeClass(kryoOut, v.getClass());
                ((KryoSerializable) v).write(kryo, kryoOut);
            } else {
                kryo.writeClassAndObject(kryoOut, v);
            }
        });
        long size = kryoOut.total();
        kryoOut.close();
        return size;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (!dataRef.isEmpty()) {
            saveData();
        }
    }

    private void saveData() {
        String workerTasks = System.getProperty("new-worker-assignment");
        LOG.info("new-worker-assignment: {}", workerTasks);
        boolean needMigration = workerTasks == null || workerTasks.equals("") ? true :
                Stream.of(workerTasks.split(",")).mapToInt(Integer::parseInt).noneMatch(i -> myTaskId == i);
        if (!needMigration) {
            DATA_HOLDER.put(myTaskId, dataRef);
            LOG.info("Put task-{} data to memory DATA_HOLDER", myTaskId);
        }
//        else if (loaclDataPath != null) {
//            try {
//                writeData(Files.newOutputStream(loaclDataPath));
//            } catch (IOException e) {
//                LOG.info("Save data failed", e);
//            }
//        }
    }

}
