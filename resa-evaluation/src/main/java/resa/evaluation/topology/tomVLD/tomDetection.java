package resa.evaluation.topology.tomVLD;

import backtype.storm.Config;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_imgproc;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FrameGrabber;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static resa.evaluation.topology.tomVLD.StormConfigManager.*;

/**
 * Created by Tom.fu on 10/3/2015.
 */
public class tomDetection {

    public static void main(String[] args) {
        //String sourceFolder = "C:\\Users\\Tom.fu\\Desktop\\VLD\\picture";
        //String outputFolder = "C:\\Users\\Tom.fu\\Desktop\\VLD\\output";

        String outputFolder = args[1];
        //String srcVideoFile = "C:\\Users\\Tom.fu\\Desktop\\VLD\\Video\\1.mp4";

        Config conf = null;
        try {
            conf = readConfig(args[0]);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int startFrame = getInt(conf, "firstFrameId");
        int endFrame = getInt(conf, "lastFrameId");
        String SOURCE_FILE = getString(conf, "videoSourceFile");

        //int startFrame = 34500;
        //int endFrame = 37000;

        int minNumberOfMatches = 4;
        int maxAdditionTemp = 4;
        //int sampleRate = 1;
        int sampleRate = Integer.parseInt(args[2]);
        boolean toDraw = Boolean.parseBoolean(args[3]);
        //List<String> templateFiles = new ArrayList<>();
        List<String> templateFiles = getListOfStrings(conf, "originalTemplateFileNames");

        //templateFiles.add("C:\\Users\\Tom.fu\\Desktop\\VLD\\logo\\mc2.jpg");
        //templateFiles.add("C:\\Users\\Tom.fu\\Desktop\\VLD\\logo\\adidas.jpg");

        long startTime = System.currentTimeMillis();
        System.out.println("new test, start at: " + startTime + ", stFrame: " + startFrame + ", endFrame: " + endFrame
                + ", sR: " + sampleRate + ", toDraw: " + toDraw + ", outputFolder: " + outputFolder);

        //LogoDetectionByInputImages(sourceFolder, outputFolder, startFrame, endFrame, maxAdditionTemp, minNumberOfMatches, templateFiles, sampleRate, true);
        LogoDetectionByInputVideo(SOURCE_FILE, outputFolder, startFrame, endFrame, maxAdditionTemp, minNumberOfMatches, templateFiles, sampleRate, toDraw);

        long endTime = System.currentTimeMillis();
        System.out.println("finished with duration: " + (endTime - startTime));
    }

    public static void LogoDetectionByInputVideo
            (String sourceFile, String outputFolder, int startFrame, int endFrame, int maxAdditionTemp,
             int minNumberOfMatches, List<String> templateFiles, int sampleRate, boolean toFile) {

        FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(sourceFile);

        Parameters parameters = new Parameters().withMatchingParameters(
                new Parameters.MatchingParameters().withMinimalNumberOfMatches(minNumberOfMatches));

        List<StormVideoLogoDetectorBeta> detectors = new ArrayList<>();
        for (int logoIndex = 0; logoIndex < templateFiles.size(); logoIndex ++) {
            detectors.add(new StormVideoLogoDetectorBeta(parameters, templateFiles.get(logoIndex), logoIndex, maxAdditionTemp));
        }

        List<opencv_core.CvScalar> colorList = new ArrayList<>();
        colorList.add(opencv_core.CvScalar.MAGENTA);
        colorList.add(opencv_core.CvScalar.YELLOW);
        colorList.add(opencv_core.CvScalar.CYAN);
        colorList.add(opencv_core.CvScalar.BLUE);
        colorList.add(opencv_core.CvScalar.GREEN);
        colorList.add(opencv_core.CvScalar.RED);
        colorList.add(opencv_core.CvScalar.BLACK);

        //StormVideoLogoDetector detector = new StormVideoLogoDetector(parameters, templateFiles);

        int frameId = 0;
        long totalFrameUsed = 0;
        try {
            grabber.start();
            while (++frameId < startFrame)
                grabber.grab();

        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        }

        int diff = endFrame - startFrame + 1;
        frameId = 0;
        endFrame = frameId + diff;

        List<Serializable.PatchIdentifier> patchIdentifierList = new ArrayList<>();
        double fx = .25, fy = .25;
        double fsx = .5, fsy = .5;

        ///int W = mat.cols(), H = mat.rows();
        int W = 640, H = 480;
        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);

        for (int x = 0; x + w <= W; x += dx) {
            for (int y = 0; y + h <= H; y += dy) {
                patchIdentifierList.add(new Serializable.PatchIdentifier(frameId, new Serializable.Rect(x, y, w, h)));
            }
        }
        System.out.println("W: " + W + ", H: " + H + ", total patch: " + patchIdentifierList.size());

        List<List<Serializable.Rect>> foundedRectList = new ArrayList<>();
        for (int logoIndex = 0; logoIndex < detectors.size(); logoIndex ++){
            foundedRectList.add(new ArrayList<>());
        }

        long start = System.currentTimeMillis();

        while (frameId < endFrame) {
            try {
                long frameStart = System.currentTimeMillis();

                opencv_core.IplImage image = grabber.grab();
                opencv_core.Mat matOrg = new opencv_core.Mat(image);
                opencv_core.Mat mat = new opencv_core.Mat();
                opencv_core.Size size = new opencv_core.Size(W, H);
                opencv_imgproc.resize(matOrg, mat, size);

                if (frameId % sampleRate == 0) {
                    foundedRectList = LogoDetectionForOneFrame(frameId, mat, detectors, patchIdentifierList);
                }

                for (int logoIndex = 0; logoIndex < foundedRectList.size(); logoIndex++) {
                    opencv_core.CvScalar color = colorList.get(logoIndex % colorList.size());
                    if (foundedRectList.get(logoIndex) != null) {
                        for (Serializable.Rect rect : foundedRectList.get(logoIndex)) {
                            Util.drawRectOnMat(rect.toJavaCVRect(), mat, color);
                        }
                    }
                }

                long frameSpend = System.currentTimeMillis() - frameStart;
                if (toFile) {
                    String outputFileName = outputFolder + System.getProperty("file.separator")
                            + String.format("frame%06d.jpg", (frameId + 1));
                    File initialImage = new File(outputFileName);
                    try {
                        BufferedImage bufferedImage = mat.getBufferedImage();
                        ImageIO.write(bufferedImage, "JPEG", initialImage);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    //System.out.println("finish draw frameID: " + frameId);
                }

                frameId++;
                long nowTime = System.currentTimeMillis();
                totalFrameUsed += frameSpend;
                System.out.println("Sendout: " + nowTime + ", " + frameId + ", used: " + (nowTime - start) + ", frameUsed: " + frameSpend + ", totalFrameUsed: " + totalFrameUsed);

            }catch (FrameGrabber.Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static void LogoDetectionByInputImages
            (String sourceFolder, String outputFolder, int startFrame, int endFrame, int maxAdditionTemp,
             int minNumberOfMatches, List<String> templateFiles, int sampleRate, boolean toFile) {

        Parameters parameters = new Parameters().withMatchingParameters(
                new Parameters.MatchingParameters().withMinimalNumberOfMatches(minNumberOfMatches));

        List<StormVideoLogoDetectorBeta> detectors = new ArrayList<>();
        for (int logoIndex = 0; logoIndex < templateFiles.size(); logoIndex ++) {
            detectors.add(new StormVideoLogoDetectorBeta(parameters, templateFiles.get(logoIndex), logoIndex, maxAdditionTemp));
        }

        List<opencv_core.CvScalar> colorList = new ArrayList<>();
        colorList.add(opencv_core.CvScalar.MAGENTA);
        colorList.add(opencv_core.CvScalar.YELLOW);
        colorList.add(opencv_core.CvScalar.CYAN);
        colorList.add(opencv_core.CvScalar.BLUE);
        colorList.add(opencv_core.CvScalar.GREEN);
        colorList.add(opencv_core.CvScalar.RED);
        colorList.add(opencv_core.CvScalar.BLACK);

        //StormVideoLogoDetector detector = new StormVideoLogoDetector(parameters, templateFiles);

        int generatedFrames = startFrame;
        int targetCount = endFrame - startFrame;

        int frameId = 0;
        long totalFrameUsed = 0;

        List<Serializable.PatchIdentifier> patchIdentifierList = new ArrayList<>();
        double fx = .25, fy = .25;
        double fsx = .5, fsy = .5;

        ///int W = mat.cols(), H = mat.rows();
        int W = 640, H = 480;
        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);

        for (int x = 0; x + w <= W; x += dx) {
            for (int y = 0; y + h <= H; y += dy) {
                patchIdentifierList.add(new Serializable.PatchIdentifier(frameId, new Serializable.Rect(x, y, w, h)));
            }
        }
        System.out.println("W: " + W + ", H: " + H + ", total patch: " + patchIdentifierList.size());

        List<List<Serializable.Rect>> foundedRectList = new ArrayList<>();
        for (int logoIndex = 0; logoIndex < detectors.size(); logoIndex ++){
            foundedRectList.add(new ArrayList<>());
        }

        long start = System.currentTimeMillis();
        while (frameId < targetCount) {
            String fileName = sourceFolder + System.getProperty("file.separator")
                    + String.format("frame%06d.jpg", (generatedFrames + 1));
            File f = new File(fileName);
            if (f.exists() == false) {
                System.out.println("File not exist: " + fileName);
                continue;
            }
            long frameStart = System.currentTimeMillis();
            opencv_core.Mat mat = opencv_highgui.imread(fileName, opencv_highgui.CV_LOAD_IMAGE_COLOR);

            if (frameId % sampleRate == 0) {
                foundedRectList = LogoDetectionForOneFrame(frameId, mat, detectors, patchIdentifierList);
            }

            for (int logoIndex = 0; logoIndex < foundedRectList.size(); logoIndex ++) {
                opencv_core.CvScalar color = colorList.get(logoIndex % colorList.size());
                if (foundedRectList.get(logoIndex) != null) {
                    for (Serializable.Rect rect : foundedRectList.get(logoIndex)) {
                        Util.drawRectOnMat(rect.toJavaCVRect(), mat, color);
                    }
                }
            }

            long frameSpend = System.currentTimeMillis() - frameStart;
            if (toFile) {
                String outputFileName = outputFolder + System.getProperty("file.separator")
                        + String.format("frame%06d.jpg", (frameId + 1));
                File initialImage = new File(outputFileName);
                try {
                    BufferedImage bufferedImage = mat.getBufferedImage();
                    ImageIO.write(bufferedImage, "JPEG", initialImage);
                }catch (IOException e) {
                    e.printStackTrace();
                }
                //System.out.println("finish draw frameID: " + frameId);
            }

            frameId++;
            generatedFrames++;

            long nowTime = System.currentTimeMillis();
            totalFrameUsed += frameSpend;
            System.out.println("Sendout: " + nowTime + ", " + frameId + ", used: " + (nowTime - start)  + ", frameUsed: " + frameSpend + ", totalFrameUsed: " + totalFrameUsed );

        }
    }

    public static List<List<Serializable.Rect>> LogoDetectionForOneFrame(
            int frameId, opencv_core.Mat mat, List<StormVideoLogoDetectorBeta> detectors,
            List<Serializable.PatchIdentifier> patchIdentifierList) {

        List<List<Serializable.Rect>> foundedRectList = new ArrayList<>();

        for (int logoIndex = 0; logoIndex < detectors.size(); logoIndex ++){
            foundedRectList.add(new ArrayList<>());
        }

        for (int logoIndex = 0; logoIndex < detectors.size(); logoIndex ++) {
            StormVideoLogoDetectorBeta detector = detectors.get(logoIndex);
            int totalFoundCnt = 0;
            for (Serializable.PatchIdentifier hostPatch : patchIdentifierList) {
                detector.detectLogosInRoi(mat, hostPatch.roi.toJavaCVRect());
                Serializable.Rect detectedLogo = detector.getFoundRect();
                Serializable.Mat extractedTemplate = detector.getExtractedTemplate();

                if (detectedLogo != null) {
                    detector.addTemplate(hostPatch, extractedTemplate);
                    detector.incrementPriority(detector.getParentIdentifier(), 1);

                    foundedRectList.get(logoIndex).add(detectedLogo);
                    totalFoundCnt++;
                }
            }
            System.out.println("FrameID: " + frameId + ", logoIndex: " + logoIndex + ", totalfoundRect: " + totalFoundCnt);
        }
        return foundedRectList;
    }
}
