package resa.evaluation.topology.tomVLD;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_nonfree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static resa.evaluation.topology.tomVLD.Constants.*;
import static resa.evaluation.topology.tomVLD.StormConfigManager.getInt;
import static resa.evaluation.topology.tomVLD.StormConfigManager.getListOfStrings;

/**
 * Created by Tom Fu at Aug 5, 2015
 * We try to enable to detector more than one logo template file
 * Enable sampling, add sampleID
 * move to Fox version
 */
public class FeatureExtraForExp extends BaseRichBolt {
    OutputCollector collector;
    opencv_nonfree.SIFT sift;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        int minNumberOfMatches = Math.min(getInt(map, "minNumberOfMatches"), 4);
        this.collector = outputCollector;
        // TODO: get path to logos & parameters from config, different logo can use different threshold?
        Parameters parameters = new Parameters()
                .withMatchingParameters(
                        new Parameters.MatchingParameters()
                                .withMinimalNumberOfMatches(minNumberOfMatches)
                );

        sift = new opencv_nonfree.SIFT(0, 3, parameters.getSiftParameters().getContrastThreshold(),
                parameters.getSiftParameters().getEdgeThreshold(), parameters.getSiftParameters().getSigma());

        System.out.println("FeatureExtraForExp.prepare");
        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    public void execute(Tuple tuple) {

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        int sampleID = tuple.getIntegerByField(FIELD_SAMPLE_ID);

        Serializable.PatchIdentifierMat identifierMat = (Serializable.PatchIdentifierMat) tuple.getValueByField(FIELD_PATCH_FRAME_MAT);
        int patchCount = tuple.getIntegerByField(FIELD_PATCH_COUNT);

        SIFTfeatures sifTfeatures = new SIFTfeatures(sift, identifierMat.sMat.toJavaCVMat(), identifierMat.identifier.roi.toJavaCVRect(), false);

        Serializable.KeyPoint sKeyPoints = sifTfeatures.keyPoints == null ? null: new Serializable.KeyPoint(sifTfeatures.keyPoints);
        if (sifTfeatures.testDescriptors != null && sifTfeatures.testDescriptors.arraySize() > 0){
            System.out.println(sifTfeatures.testDescriptors.rows() + ", " + sifTfeatures.testDescriptors.cols() + ", "
                    + sifTfeatures.testDescriptors.arraySize() + ", " + (sifTfeatures.testDescriptors.getByteBuffer() == null));
        }
        Serializable.Mat sTestDescriptors = sifTfeatures.testDescriptors == null ? null : new Serializable.Mat(sifTfeatures.testDescriptors);
        if (sifTfeatures.rr != null){
            System.out.println(sifTfeatures.rr.rows() + ", " + sifTfeatures.rr.cols() + ", "
                    + sifTfeatures.rr.arraySize() + ", " + (sifTfeatures.rr.getByteBuffer() == null));
        }
        Serializable.Mat sRR = sifTfeatures.rr == null ? null : new Serializable.Mat(sifTfeatures.rr);
        Serializable.Rect sRoi = sifTfeatures.roi == null? null : new Serializable.Rect(sifTfeatures.roi);

        collector.emit(SIFT_FEATURE_STREAM, tuple, new Values(frameId, sKeyPoints, sTestDescriptors, sRR, sRoi, identifierMat.identifier, patchCount, sampleID));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(SIFT_FEATURE_STREAM,
                new Fields(FIELD_FRAME_ID, FIELD_SIFT_KEY_POINTS, FIELD_SIFT_TDES_MAT, FIELD_SIFT_RR_MAT, FIELD_SIFT_ROI, FIELD_PATCH_IDENTIFIER, FIELD_PATCH_COUNT, FIELD_SAMPLE_ID));
    }
}
