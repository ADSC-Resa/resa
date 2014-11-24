package resa.evaluation.tools;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.FFmpegFrameGrabber;

import static org.bytedeco.javacpp.opencv_core.cvSize;
import static org.bytedeco.javacpp.opencv_highgui.cvSaveImage;
import static org.bytedeco.javacpp.opencv_imgproc.cvResize;

/**
 * Created by ding on 14/10/18.
 */
public class Video2Image2 {

    public static void main(String[] args) throws Exception {
        FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(args[0]);
        grabber.start();
        int i = 0;
        opencv_core.IplImage img = null;
        int max = Integer.parseInt(args[2]);
        if (max <= 0) {
            max = Integer.MAX_VALUE;
        }
        while (i++ < max) {
            opencv_core.IplImage source = grabber.grab();
            if (img == null) {
                opencv_core.CvSize size = cvSize(source.width() / 2, source.height() / 2);
                img = opencv_core.IplImage.create(size, source.depth(), source.nChannels());
            }
            cvResize(source, img);
            cvSaveImage(String.format(args[1] + "/%05d.jpg", i), img);
        }
        System.out.println("done, got " + i + " images");
    }
}
