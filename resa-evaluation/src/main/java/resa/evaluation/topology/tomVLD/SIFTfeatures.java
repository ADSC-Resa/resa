package resa.evaluation.topology.tomVLD;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_features2d;
import org.bytedeco.javacpp.opencv_nonfree;

/**
 * Created by Tom.fu on 29/4/2015.
 */
public class SIFTfeatures {

    opencv_features2d.KeyPoint keyPoints; //= new opencv_features2d.KeyPoint();
    opencv_core.Mat testDescriptors;// = new opencv_core.Mat();
    opencv_core.Mat rr;// = subFrame.clone();
    opencv_core.Rect roi;

    public SIFTfeatures(opencv_nonfree.SIFT sift, opencv_core.Mat frame, opencv_core.Rect rec, boolean isWholeFrame){

        keyPoints = new opencv_features2d.KeyPoint();
        testDescriptors = new opencv_core.Mat();
        roi = new opencv_core.Rect();
        roi.x(rec.x());
        roi.y(rec.y());
        roi.width(rec.width());
        roi.height(rec.height());

        if (isWholeFrame) {
            opencv_core.Mat r = new opencv_core.Mat(frame, roi);
            rr = r.clone(); // make r continuous
            r.release();
        }else {
            rr = frame.clone();//subframe
        }

        sift.detectAndCompute(rr, opencv_core.Mat.EMPTY, keyPoints, testDescriptors);
    }

    public SIFTfeatures(Serializable.KeyPoint sKeyPoints, Serializable.Mat sTestDescriptors, Serializable.Mat sRR, Serializable.Rect sRoi){
        keyPoints = sKeyPoints == null ? null : sKeyPoints.toJavaCvFeatures2dKeyPoint();
        testDescriptors = sTestDescriptors == null ? null :  sTestDescriptors.toJavaCVMat();
        rr = sRR == null ? null : sRR.toJavaCVMat();
        roi = sRoi == null ? null : sRoi.toJavaCVRect();
    }

    public void release(){
        // Manually force JVM to release this.
        rr.release();
        keyPoints.deallocate();
        testDescriptors.release();
    }
}
