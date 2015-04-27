package resa.evaluation.topology.tomVLD;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

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
 */
public class tomVLDTopExpFInBC {

    public static void main(String args[]) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = readConfig(args[0]);

        TopologyBuilder builder = new TopologyBuilder();
        String spoutName = "tVLDSpout";
        String patchGenBolt = "tVLDPatchGen";
        String patchProcBolt = "tVLDPatchProc";
        String patchAggBolt = "tVLDPatchAgg";
        String patchDrawBolt = "tVLDPatchDraw";
        String redisFrameOut = "tVLDRedisFrameOut";

        builder.setSpout(spoutName, new tomFrameSpoutResize(), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        builder.setBolt(patchGenBolt, new tomPatchGenWSampleBC(), getInt(conf, patchGenBolt + ".parallelism"))
                .shuffleGrouping(spoutName, RAW_FRAME_STREAM) //TODO: notice, there is a bug not fixed, when sample rate is > 1
                .setNumTasks(getInt(conf, patchGenBolt + ".tasks"));

        builder.setBolt(patchProcBolt, new PatchProcessorBoltMultiple(), getInt(conf, patchProcBolt + ".parallelism"))
                .allGrouping(patchProcBolt, LOGO_TEMPLATE_UPDATE_STREAM)
                .allGrouping(patchAggBolt, CACHE_CLEAR_STREAM)
                .shuffleGrouping(patchGenBolt, PATCH_STREAM)
                .allGrouping(patchGenBolt, RAW_FRAME_STREAM)
                .setNumTasks(getInt(conf, patchProcBolt + ".tasks"));

        builder.setBolt(patchAggBolt, new PatchAggBoltMultipleBeta(), getInt(conf, patchAggBolt + ".parallelism"))
                .globalGrouping(patchProcBolt, DETECTED_LOGO_STREAM)
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
        //conf.registerSerialization(Serializable.Mat.class);
        int sampleFrames = getInt(conf, "sampleFrames");
        int W = ConfigUtil.getInt(conf, "width", 640);
        int H = ConfigUtil.getInt(conf, "height", 480);

        List<String> templateFiles = getListOfStrings(conf, "originalTemplateFileNames");

        StormSubmitter.submitTopology("tomVLDTopExpFInBC-s" + sampleFrames + "-" + W + "-" + H + "-L" + templateFiles.size(), conf, topology);

    }
}
