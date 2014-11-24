package resa.evaluation.tools;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;

import static org.bytedeco.javacpp.opencv_core.cvSize;
import static org.bytedeco.javacpp.opencv_highgui.cvSaveImage;
import static org.bytedeco.javacpp.opencv_imgproc.cvResize;

/**
 * Created by ding on 14/10/18.
 */
public class Video2Image {

    public static void main(String[] args) {
        opencv_highgui.VideoCapture capture = new opencv_highgui.VideoCapture(args[0]);
        int i = 0;
        opencv_core.IplImage img = null;
        int max = Integer.parseInt(args[2]);
        if (max <= 0) {
            max = Integer.MAX_VALUE;
        }
        opencv_core.Mat mat = new opencv_core.Mat();
        while (i++ < max && capture.read(mat)) {
            opencv_core.IplImage source = mat.asIplImage();
            if (img == null) {
                opencv_core.CvSize size = cvSize(source.width() / 2, source.height() / 2);
                img = opencv_core.IplImage.create(size, source.depth(), source.nChannels());
            }
            cvResize(source, img);
            cvSaveImage(String.format(args[1] + "/%05d.jpg", i), img);
        }
        capture.release();
        System.out.println("done, got " + i + " images");
    }
}
