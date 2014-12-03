package resa.evaluation.simulate;

import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import resa.examples.wc.WordCountTopology;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by ding on 14-7-29.
 */
public class HdfsDataLoader extends WordCountTopology.WordCount {

    private Configuration hdfsConf;

    private static class WordCountDB implements KryoSerializable {
        private long size;

        public WordCountDB(long size) {
            this.size = size;
        }

        public WordCountDB() {
        }

        @Override
        public void write(Kryo kryo, Output output) {
            String s = UUID.randomUUID().toString();
            output.writeLong(size);
            long total = output.total();
            while (output.total() - total < size) {
                output.writeBoolean(true);
                output.writeString(s);
                output.writeInt((int) (Math.random() * Integer.MAX_VALUE));
            }
            output.writeBoolean(false);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            size = input.readLong();
            while (input.readBoolean()) {
                input.readString();
                input.readInt();
            }
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.hdfsConf = new Configuration();
        if (context.getTaskData("pattern") == null) {
            try {
                generateData(stormConf, context);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            Path file = getPath(stormConf, context);
            if (!dataFileExist(file)) {
                context.getSharedExecutor().submit(() -> {
                    Kryo kryo = SerializationFactory.getKryo(stormConf);
                    try (Output out = new Output(FileSystem.get(hdfsConf).create(getPath(stormConf, context), true,
                            12 * 1024, (short) 2, 32 * 1024 * 1024L))) {
                        out.writeInt(1);
                        out.writeString("pattern");
                        WordCountDB db = (WordCountDB) context.getTaskData("pattern");
                        kryo.writeClass(out, db.getClass());
                        db.write(kryo, out);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    private boolean dataFileExist(Path file) {
        try {
            return FileSystem.get(hdfsConf).exists(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void generateData(Map stormConf, TopologyContext context) throws IOException {
        List<Double> dataSizes = (List<Double>) stormConf.get("dataSizes");
        long dataSize = (long) (dataSizes.get(context.getThisTaskIndex()) * 600);
        Kryo kryo = SerializationFactory.getKryo(stormConf);
        WordCountDB db = new WordCountDB(dataSize);
        try (Output out = new Output(FileSystem.get(hdfsConf).create(getPath(stormConf, context), true,
                12 * 1024, (short) 2, 32 * 1024 * 1024L))) {
            out.writeInt(1);
            out.writeString("pattern");
            kryo.writeClass(out, db.getClass());
            db.write(kryo, out);
        }
        System.out.println("Load dataSize is " + dataSize);
        context.setTaskData("pattern", db);
    }

    private Path getPath(Map<String, Object> conf, TopologyContext context) {
        Path dataPath = new Path(String.format("/resa/%s/task-%03d.data", context.getStormId(),
                context.getThisTaskId()));
        try {
            FileSystem fs = FileSystem.get(hdfsConf);
            if (!fs.exists(dataPath.getParent())) {
                fs.mkdirs(dataPath.getParent());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataPath;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
//        long now = System.currentTimeMillis();
//        do {
        for (int i = 0; i < 10; i++) {
            Math.atan(Math.sqrt(Math.random() * Integer.MAX_VALUE));
        }
//        } while (System.currentTimeMillis() - now > 1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
