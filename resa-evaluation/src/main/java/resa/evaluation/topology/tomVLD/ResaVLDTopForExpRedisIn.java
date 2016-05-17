package resa.evaluation.topology.tomVLD;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import resa.evaluation.topology.vld.FeatureExtracterCharlie;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ResaConfig;

import java.io.FileNotFoundException;
import java.util.List;

import static resa.evaluation.topology.tomVLD.Constants.*;
import static resa.evaluation.topology.tomVLD.StormConfigManager.*;

/**
 * Created by Tom Fu, a new version based on echoOneGenRIRO!!
 * In the delta version, we enables the feature of supporting the multiple logo input,
 * When the setting in the configuration file includes multiple logo image files,
 * it automatically creates corresponding detector instance
 * Note: we in this version's patchProc bolt, uses the StormVideoLogoDetectorGamma
 * This gamma Detector helps to decrease overhead of multiple logo image files.
 *
 * Through testing, when sampleFrame = 4, it supports up to 25 fps.
 * Updated on April 29, the way to handle frame sampling issue is changed, this is pre-processed by the spout not to
 * send out unsampled frames to the patch generation bolt.
 *
 * Enabling sampling features. Sampling problem is solved!
 * through testing
 *
 * Write on Dec 15, 2015
 * TODO: some new improvement point: 1. re-design of redisFrameOutput, the sorting queue can be moved out to the explicit programme.
 * TODO: the output can be frameID + frame to the redis queue, (need to modify serializableMat), so that when the explicit programme pop from redis queue, it can do the sorting
 * TODO: a question? can we re-write serializableMat, which is extended from opencv_core.Mat, implements io.serializable and kysto?
 */
public class ResaVLDTopForExpRedisIn {


    public static void main(String args[]) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = readConfig(args[0]);
        String host = getString(conf, "input.redis.host");
        int port = getInt(conf, "input.redis.port");
        String queueName = getString(conf, "input.redis.queueName");

        TopologyBuilder builder = new ResaTopologyBuilder();
//        TopologyBuilder builder = new TopologyBuilder();
        String spoutName = "tVLDSpout";
        String patchGenBolt = "tVLDPatchGen";
        String feaExtraBolt = "tVLDFeaExtra";
        String patchProcBolt = "tVLDPatchProc";
        String patchAggBolt = "tVLDPatchAgg";
        String patchDrawBolt = "tVLDPatchDraw";
        String redisFrameOut = "tVLDRedisFrameOut";

        builder.setSpout(spoutName, new FrameSourceFox(host,port,queueName), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        builder.setBolt(patchGenBolt, new PatchGenFox(), getInt(conf, patchGenBolt + ".parallelism"))
                .shuffleGrouping(spoutName, SAMPLE_FRAME_STREAM)
                .setNumTasks(getInt(conf, patchGenBolt + ".tasks"));

        builder.setBolt(feaExtraBolt, new FeatureExtraForExp(), getInt(conf, patchProcBolt + ".parallelism"))
                .shuffleGrouping(patchGenBolt, PATCH_FRAME_STREAM)
                .setNumTasks(getInt(conf, feaExtraBolt + ".tasks"));

        builder.setBolt(patchProcBolt, new PatchProcessorForExp(), getInt(conf, patchProcBolt + ".parallelism"))
                .allGrouping(patchProcBolt, LOGO_TEMPLATE_UPDATE_STREAM)
                .shuffleGrouping(feaExtraBolt, SIFT_FEATURE_STREAM)
                .setNumTasks(getInt(conf, patchProcBolt + ".tasks"));

        builder.setBolt(patchAggBolt, new PatchAggFox(), getInt(conf, patchAggBolt + ".parallelism"))
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

        StormSubmitter.submitTopology("ResaVLDTopForExpRedisIn-s"
                + sampleFrames + "-" + W + "-" + H + "-L" + templateFiles.size() + "-p" + maxPending, resaConfig, topology);

    }
}
