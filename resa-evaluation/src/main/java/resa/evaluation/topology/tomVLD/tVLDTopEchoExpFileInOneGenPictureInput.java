package resa.evaluation.topology.tomVLD;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ResaConfig;

import java.util.List;

import static resa.evaluation.topology.tomVLD.Constants.*;
import static resa.evaluation.topology.tomVLD.StormConfigManager.*;

/**
 * Created by Tom Fu, this version is through basic testing.
 * In the delta version, we enables the feature of supporting the multiple logo input,
 * When the setting in the configuration file includes multiple logo image files,
 * it automatically creates corresponding detector instance
 *
 * When the number of logo image file = 1, it turn back to the gamma version.
 * This version is still preliminary and need more improvement
 *
 * Note: we in this version's patchProc bolt (tPatchProcessorDelta), uses the StormVideoLogoDetectorBeta class, not the normal one StormVideoLogoDetector!!!
 * Through testing, when sampleFrame = 4, it supports up to 25 fps.
 *
 */
public class tVLDTopEchoExpFileInOneGenPictureInput {

    //TODO: further improvement: a) re-design PatchProcessorBolt, this is too heavy loaded!
    // b) then avoid broadcast the whole frames, split the functions in PatchProcessorBolt.
    //

    public static void main(String args[]) throws Exception {
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

        builder.setSpout(spoutName, new tomFrameSpoutResizePictureInput(), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        builder.setBolt(patchGenBolt, new PatchGeneraterWSampleOneStep(), getInt(conf, patchGenBolt + ".parallelism"))
                .shuffleGrouping(spoutName, RAW_FRAME_STREAM) //TODO: notice, there is a bug not fixed, when sample rate is > 1
                .setNumTasks(getInt(conf, patchGenBolt + ".tasks"));

        builder.setBolt(patchProcBolt, new tPatchProcessorEcho(), getInt(conf, patchProcBolt + ".parallelism"))
                .allGrouping(patchProcBolt, LOGO_TEMPLATE_UPDATE_STREAM)
                .shuffleGrouping(patchGenBolt, PATCH_FRAME_STREAM)
                .setNumTasks(getInt(conf, patchProcBolt + ".tasks"));

        builder.setBolt(patchAggBolt, new tPatchAggSampleDelta(), getInt(conf, patchAggBolt + ".parallelism"))
                .fieldsGrouping(patchProcBolt, DETECTED_LOGO_STREAM, new Fields(FIELD_FRAME_ID))
                //.globalGrouping(patchProcBolt, DETECTED_LOGO_STREAM)
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

        StormSubmitter.submitTopology("tVLDEchoExpFInOG-s"
                + sampleFrames + "-" + W + "-" + H + "-L" + templateFiles.size() + "-p" + maxPending, resaConfig, topology);

    }
}
