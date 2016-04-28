package resa.evaluation.topology.tomVLD;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ResaConfig;

import java.io.FileNotFoundException;
import java.util.List;

import static resa.evaluation.topology.tomVLD.Constants.*;
import static resa.evaluation.topology.tomVLD.StormConfigManager.*;

/**
 * Created by Tom Fu, this version is through basic testing.
 *
 * This is for experiment purpose  (results for paper)
 * This is use the very original version by Nurlan,
 * the spout (or patchGen) broadcasts the raw frames to all the patchProcessingBolt, and shuffles those patch identifiers.
 * In this version, we fixed the sample bug
 */
public class ResaVLDTopFoxRedisInBC {

    public static void main(String args[]) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = readConfig(args[0]);
        String host = getString(conf, "redis.host");
        int port = getInt(conf, "redis.port");
        String queueName = getString(conf, "tVLDQueueName");

        TopologyBuilder builder = new ResaTopologyBuilder();
        //TopologyBuilder builder = new TopologyBuilder();
        String spoutName = "tVLDSpout";
        String patchGenBolt = "tVLDPatchGen";
        String patchProcBolt = "tVLDPatchProc";
        String patchAggBolt = "tVLDPatchAgg";
        String patchDrawBolt = "tVLDPatchDraw";
        String redisFrameOut = "tVLDRedisFrameOut";

        builder.setSpout(spoutName, new FrameSourceFox(host,port,queueName), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        builder.setBolt(patchGenBolt, new tomPatchGenWSampleBC(), getInt(conf, patchGenBolt + ".parallelism"))
                .shuffleGrouping(spoutName, SAMPLE_FRAME_STREAM)
                .setNumTasks(getInt(conf, patchGenBolt + ".tasks"));

        builder.setBolt(patchProcBolt, new PatchProcessorBoltMultipleEcho(), getInt(conf, patchProcBolt + ".parallelism"))
                .allGrouping(patchProcBolt, LOGO_TEMPLATE_UPDATE_STREAM)
                .allGrouping(patchAggBolt, CACHE_CLEAR_STREAM)
                .shuffleGrouping(patchGenBolt, PATCH_STREAM)
                .allGrouping(patchGenBolt, SAMPLE_FRAME_STREAM)
                .setNumTasks(getInt(conf, patchProcBolt + ".tasks"));

        builder.setBolt(patchAggBolt, new PatchAggBoltMultipleBeta(), getInt(conf, patchAggBolt + ".parallelism"))
                .fieldsGrouping(patchProcBolt, DETECTED_LOGO_STREAM, new Fields(FIELD_SAMPLE_ID))
                .setNumTasks(getInt(conf, patchAggBolt + ".tasks"));

        builder.setBolt(patchDrawBolt, new tDrawPatchDelta(), getInt(conf, patchDrawBolt + ".parallelism"))
                .fieldsGrouping(patchAggBolt, PROCESSED_FRAME_STREAM, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping(spoutName, RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, patchDrawBolt + ".tasks"));

        builder.setBolt(redisFrameOut, new RedisFrameOutput(), getInt(conf, redisFrameOut + ".parallelism"))
                .globalGrouping(patchDrawBolt, STREAM_FRAME_DISPLAY)
                .setNumTasks(getInt(conf, redisFrameOut + ".tasks"));

        StormTopology topology = builder.createTopology();

        int numberOfWorkers = getInt(conf, "tVLDNumOfWorkers");
        conf.setNumWorkers(numberOfWorkers);
        conf.setMaxSpoutPending(getInt(conf, "tVLDMaxPending"));
        conf.setStatsSampleRate(1.0);
        conf.registerSerialization(Serializable.Mat.class);

        List<String> templateFiles = getListOfStrings(conf, "originalTemplateFileNames");
        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);

        if (resa.util.ConfigUtil.getBoolean(conf, "tVLD.metric.resa", false)) {
            resaConfig.addDrsSupport();
            resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
            System.out.println("ResaMetricsCollector is registered");
        }

        if (resa.util.ConfigUtil.getBoolean(conf, "tVLD.metric.redis", false)) {
            resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
            System.out.println("RedisMetricsCollector is registered");
        }

        int sampleFrames = getInt(resaConfig, "sampleFrames");
        int W = ConfigUtil.getInt(resaConfig, "width", 640);
        int H = ConfigUtil.getInt(resaConfig, "height", 480);
        int maxPending = getInt(resaConfig, "topology.max.spout.pending");

        StormSubmitter.submitTopology("resaVLDTopFoxFileInBC-s"
                + sampleFrames + "-" + W + "-" + H + "-L" + templateFiles.size() + "-p" + maxPending, resaConfig, topology);

    }
}
