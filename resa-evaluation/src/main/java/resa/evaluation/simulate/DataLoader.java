package resa.evaluation.simulate;

import backtype.storm.Config;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import resa.examples.wc.WordCountTopology;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by ding on 14-7-29.
 */
public class DataLoader extends WordCountTopology.WordCount {

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
//                for (int i = 0; i < 1; i++) {
//                    Math.atan(Math.sqrt(Math.random() * Integer.MAX_VALUE));
//                }
                input.readString();
                input.readInt();
            }
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        if (context.getTaskData("pattern") == null) {
            try {
                generateData(stormConf, context);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            Path file = getPath(stormConf, context);
            if (!Files.exists(file)) {
                context.getSharedExecutor().submit(() -> {
                    Kryo kryo = SerializationFactory.getKryo(stormConf);
                    try (Output out = new Output(Files.newOutputStream(getPath(stormConf, context)))) {
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

    public void generateData(Map stormConf, TopologyContext context) throws IOException {
        List<Double> dataSizes = (List<Double>) stormConf.get("dataSizes");
        long dataSize = (long) (dataSizes.get(context.getThisTaskIndex()) * 600);
        Kryo kryo = SerializationFactory.getKryo(stormConf);
        WordCountDB db = new WordCountDB(dataSize);
        try (Output out = new Output(Files.newOutputStream(getPath(stormConf, context)))) {
            out.writeInt(1);
            out.writeString("pattern");
            kryo.writeClass(out, db.getClass());
            db.write(kryo, out);
        }
        System.out.println("Load dataSize is " + dataSize);
        context.setTaskData("pattern", db);
    }

    private Path getPath(Map<String, Object> conf, TopologyContext context) {
        Path loaclDataPath = Paths.get((String) conf.get(Config.STORM_LOCAL_DIR), "data", context.getStormId());
        if (!Files.exists(loaclDataPath)) {
            try {
                Files.createDirectories(loaclDataPath);
            } catch (FileAlreadyExistsException e) {
            } catch (IOException e) {
                loaclDataPath = null;
            }
        }
        if (loaclDataPath != null) {
            loaclDataPath = loaclDataPath.resolve(String.format("task-%03d.data", context.getThisTaskId()));
        }
        return loaclDataPath;
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
