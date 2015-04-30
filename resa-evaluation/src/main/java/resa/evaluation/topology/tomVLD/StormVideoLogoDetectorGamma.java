package resa.evaluation.topology.tomVLD;


import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.Rect;
import org.bytedeco.javacpp.opencv_features2d.KeyPoint;
import org.bytedeco.javacpp.opencv_nonfree.SIFT;

import java.util.ArrayList;
import java.util.Collections;

import static org.bytedeco.javacpp.opencv_highgui.cvLoadImage;

/**
 * This is a beta version, which enables the feature of multiple logo template input.
 * One detector actually maps to one log template, the multiple detector instance should be created at higher level.
 * TODO: some of the process and algorithm can be further improved to be more efficient, e.g, can we involve all the logo template in one detector?
 * This is a detector, which stores logo templates ({@link LogoTemplate}),
 * and, given a patch of the frame, attempts to find a logo on it by matching it against each logo template.
 * Logo templates are checked in order of their decreasing priority. They are stored in two separate lists:
 * {@link #originalTemp} and {@link #addedTempList}. The first one contains static original logo templates,
 * the second one, initially empty, contains dynamically added logo templates.
 * <p>
 * ==============================Definitions==========================<p>
 * 1. Query image, query logo, logo template - images of logos we are looking for.
 * <p>
 * 2. Train image, frame image - image of the frame in which we are looking for logos.
 * <p>
 * 3. Homography matrix, homography - 3x3 transformation matrix, which projects our logo template onto the
 * frame space. Usually we project the corners of our template logo onto a frame to obtain the
 * quadrilateral, which determines the locations of the detected logo.
 * <p>
 * 4. RoI, roi - region of interest. Usually this is a rectangle corresponding to the part of the image
 * which we are examining.
 * <p>
 * 5. Original, Static logo templates - those that were added at the beginning.
 * <p>
 * 6. Added, Dynamic logo templates - those that were added during the detection.
 * <p>
 * Usage:<p>
 * 1. Initialize detector with original logo templates {@link LogoTemplate}.
 * Also specify Parameters {@link Parameters}
 * for detections such as parameters for SIFT feature extractor, RANSAC algorithm and others.
 * <p>
 * 2. Feed patches of the frames to the detector one by one using
 * {@link #detectLogosInRoi(Mat, Rect)}
 * <p>
 * 3. If detector detected some logo on the patch you can access its rectangle by {@link #getFoundRect()}. If
 * {@link #getFoundRect()} returns null, then no logo has been detected on this patch.
 * <p>
 * 4. If you wish to add a new logo template (for example, you extracted a template from some frame), you should call
 * {@link #addTemplate(Serializable.PatchIdentifier, Serializable.Mat)}.
 * <p>
 * 5. Also you may want to increment the priority of the existing logo template. Then you call
 * {@link #incrementPriority(Serializable.PatchIdentifier, int)}.
 *
 * @see LogoTemplate
 * @see RobustMatcher
 * *
 */
public class StormVideoLogoDetectorGamma {

    /**
     * Stores original static logo templates
     */
    private LogoTemplate originalTemp;
    /**
     * Stores dynamically added logo templates
     */
    private ArrayList<LogoTemplate> addedTempList;
    /**
     * Parameters for detection
     */
    private Parameters params;
    /**
     * SIFT object for feature extraction
     */
    private SIFT sift;
    /**
     * Matcher, which performs all matching and refinement
     */
    private RobustMatcher robustMatcher;
    /**
     * If logo is found this will reference the rectangle corresponding to detected logo
     */
    private Serializable.Rect foundRect;
    /**
     * If logo is found this will reference the logo template extracted from this patch
     */
    private Serializable.Mat extractedTemplate;
    /**
     * If logo is found this will reference the template using which the logo was detected
     */
    private LogoTemplate parent;

    private int maxTemplateSize;

    /**
     * Initializes and precomputes all key points and descriptors for static template logos
     *
     * @param params   The parameters for logo detection and matching.
     * @param fileName The path to file containing images of the original logos
     */
    public StormVideoLogoDetectorGamma(Parameters params, String fileName, int logoIndex, int maxTemplateSize) {
        if (Debug.logoDetectionDebugOutput)
            System.out.println("Initializing logos...");

        // Initialize lists and matcher and sift
        this.params = params;
        addedTempList = new ArrayList<>();
        robustMatcher = new RobustMatcher(params);
        this.maxTemplateSize = maxTemplateSize;

        sift = new SIFT(0, 3, params.getSiftParameters().getContrastThreshold(),
                params.getSiftParameters().getEdgeThreshold(), params.getSiftParameters().getSigma());

        // This is the index of the original logo templates
        try {
            ///TODO: double check, Fixed by Tom fu on April, 2015, the original case always throws IO exceptions
            //Mat tmp = new Mat(IplImage.createFrom(ImageIO.read(new FileInputStream(fileName))));
            Mat tmp = new Mat(cvLoadImage(fileName));
            //opencv_core.Mat matOrg = opencv_highgui.imread(fileName, opencv_highgui.CV_LOAD_IMAGE_COLOR);
            Mat descriptor = new Mat();
            KeyPoint keyPoints = new KeyPoint();
            sift.detectAndCompute(tmp, Mat.EMPTY, keyPoints, descriptor);
            // Original templates have negative ids and null roi.
            originalTemp = new LogoTemplate(tmp, keyPoints, descriptor, new Serializable.PatchIdentifier(-logoIndex - 1, null));
        } catch (Exception e)
        //catch (IOException e)
        {
            e.printStackTrace();
            System.err.println("StormLogoDetector(): Could not open file " + fileName);
        }
        if (Debug.logoDetectionDebugOutput)
            System.out.println("Initialization of logo templates complete, id: " + logoIndex);
    }


    /**
     * This methods looks for logos on the part of the frame defined by roi.
     *
     * @param frame The image Mat containing the whole logo
     * @param roi   The rectangle corresponding the this patch
     */
    public void detectLogosInRoi(Mat frame, Rect roi) {
        // Make the results of previous detection null
        foundRect = null;
        extractedTemplate = null;
        parent = null;

        // Obtain the keypoints, descriptors of the patch
        Mat r = new Mat(frame, roi);
        KeyPoint keyPoints = new KeyPoint();
        Mat testDescriptors = new Mat();
        Mat rr = r.clone(); // make r continuous
        r.release();
        sift.detectAndCompute(rr, Mat.EMPTY, keyPoints, testDescriptors);

        // Sort list by decreasing order of priority
        if (keyPoints.capacity() >= params.getMatchingParameters().getMinimalNumberOfMatches() &&
                robustMatcher.matchImages(originalTemp.imageMat, originalTemp.descriptor, originalTemp.keyPoints,
                        rr, testDescriptors, keyPoints, roi)) {
            // If logo is found update the results and break.
            parent = originalTemp;
            foundRect = robustMatcher.getFoundRect();
            extractedTemplate = robustMatcher.getExtractedTemplate();
        }

        if (foundRect == null) {
            // If logo hasn't been yet found, sort list of dynamic templates.
//            if (addedTempList.size() > maxTemplateSize) {
//                addedTempList.remove(0);
//            }

            Collections.sort(addedTempList);
            if (addedTempList.size() > maxTemplateSize) {
                addedTempList.remove(addedTempList.size() - 1);
            }

            for (LogoTemplate lt : addedTempList) {
                if (keyPoints.capacity() >= params.getMatchingParameters().getMinimalNumberOfMatches() &&
                        robustMatcher.matchImages(lt.imageMat, lt.descriptor, lt.keyPoints,
                                rr, testDescriptors, keyPoints, roi)) {
                    // If logo is found update the results and break.
                    parent = lt;
                    foundRect = robustMatcher.getFoundRect();
                    extractedTemplate = robustMatcher.getExtractedTemplate();
                    break;
                }
            }
        }
        // Manually force JVM to release this.
        rr.release();
        keyPoints.deallocate();
        testDescriptors.release();
    }

    /**
     * This methods looks for logos on the part of the frame defined by roi, but the sub-frame is already extracted as input
     *
     * @param subFrame The image Mat containing the whole logo
     * @param roi      The rectangle corresponding the this patch
     */
    public void detectLogosInMatRoi(Mat subFrame, Rect roi) {
        // Make the results of previous detection null
        foundRect = null;
        extractedTemplate = null;
        parent = null;

        // Obtain the keypoints, descriptors of the patch
        //Mat r = new Mat(frame, roi);
        KeyPoint keyPoints = new KeyPoint();
        Mat testDescriptors = new Mat();
        Mat rr = subFrame.clone();
        //r.release();
        sift.detectAndCompute(rr, Mat.EMPTY, keyPoints, testDescriptors);

        // Sort list by decreasing order of priority
        if (keyPoints.capacity() >= params.getMatchingParameters().getMinimalNumberOfMatches() &&
                robustMatcher.matchImages(originalTemp.imageMat, originalTemp.descriptor, originalTemp.keyPoints,
                        rr, testDescriptors, keyPoints, roi)) {
            // If logo is found update the results and break.
            parent = originalTemp;
            foundRect = robustMatcher.getFoundRect();
            extractedTemplate = robustMatcher.getExtractedTemplate();
        }

        if (foundRect == null) {
            // If logo hasn't been yet found, sort list of dynamic templates.

//            if (addedTempList.size() > maxTemplateSize) {
//                addedTempList.remove(0);
//            }

            Collections.sort(addedTempList);
            if (addedTempList.size() > maxTemplateSize) {
                addedTempList.remove(addedTempList.size() - 1);
            }

            for (LogoTemplate lt : addedTempList) {
                if (keyPoints.capacity() >= params.getMatchingParameters().getMinimalNumberOfMatches() &&
                        robustMatcher.matchImages(lt.imageMat, lt.descriptor, lt.keyPoints,
                                rr, testDescriptors, keyPoints, roi)) {
                    // If logo is found update the results and break.
                    parent = lt;
                    foundRect = robustMatcher.getFoundRect();
                    extractedTemplate = robustMatcher.getExtractedTemplate();
                    break;
                }
            }
        }
        // Manually force JVM to release this.
        rr.release();
        keyPoints.deallocate();
        testDescriptors.release();
    }

    /**
     * This methods looks for logos on the part of the frame defined by roi, but the sub-frame is already extracted as input
     *
     * @param features The extracted sift feature of the patch
     */
    public void detectLogosByFeatures(SIFTfeatures features) {
        // Make the results of previous detection null
        foundRect = null;
        extractedTemplate = null;
        parent = null;

        // Sort list by decreasing order of priority
        if (features.keyPoints.capacity() >= params.getMatchingParameters().getMinimalNumberOfMatches() &&
                robustMatcher.matchImages(originalTemp.imageMat, originalTemp.descriptor, originalTemp.keyPoints,
                        features.rr, features.testDescriptors, features.keyPoints, features.roi)) {
            // If logo is found update the results and break.
            parent = originalTemp;
            foundRect = robustMatcher.getFoundRect();
            extractedTemplate = robustMatcher.getExtractedTemplate();
        }

        if (foundRect == null) {
            // If logo hasn't been yet found, sort list of dynamic templates.

//            if (addedTempList.size() > maxTemplateSize) {
//                addedTempList.remove(0);
//            }

            Collections.sort(addedTempList);
            if (addedTempList.size() > maxTemplateSize) {
                addedTempList.remove(addedTempList.size() - 1);
            }

            if (features.keyPoints.capacity() >= params.getMatchingParameters().getMinimalNumberOfMatches()) {
                for (LogoTemplate lt : addedTempList) {
                    if (robustMatcher.matchImages(lt.imageMat, lt.descriptor, lt.keyPoints,
                            features.rr, features.testDescriptors, features.keyPoints, features.roi)) {
                        // If logo is found update the results and break.
                        parent = lt;
                        foundRect = robustMatcher.getFoundRect();
                        extractedTemplate = robustMatcher.getExtractedTemplate();
                        break;
                    }
                }
            }
        }
    }

    /**
     * If logo was detected, you may get its global coordinates (relative to the whole frame)
     *
     * @return the rectangle enclosing detected logo
     */
    public Serializable.Rect getFoundRect() {
        return foundRect;
    }

    /**
     * If logo was detected, this you can get the Mat of the image, corresponding to this extracted template.
     * Usually you send it to other bolts, so that they can update their logo template list.
     *
     * @return Mat containing the image of the extracted template
     */
    public Serializable.Mat getExtractedTemplate() {
        return extractedTemplate;
    }

    /**
     * This returns the identifier of the template, using which the logo was detected, so called 'parent' template.
     * Usually you send it to other bolts, so that they update its priority.
     *
     * @return identifier of parent template.
     */
    public Serializable.PatchIdentifier getParentIdentifier() {
        return parent.identifier;
    }

    /**
     * Finds a logo template identified by given patch identifier and increments its priority by value.
     *
     * @param identifier - the patch identifier to identify the logo template, which priority needs to be updated
     * @param value      - the amount by which its priority should be updated
     * @return true if the corresponding template was found and updated and false otherwise. False may happen
     * if this particular detector hasn't been updated by the Storm with this logo template.
     */
    public boolean incrementPriority(Serializable.PatchIdentifier identifier, int value) {
        for (LogoTemplate lt : addedTempList) {
            if (lt.identifier.equals(identifier)) {
                lt.priority += value;
                return true;
            }
        }
        return false;
    }

    /**
     * Adds template to the dynamic list of logo templates.
     *
     * @param identifier The identifier of the patch, from which this template was extracted.
     * @param mat        Image of the logo template
     */
    public void addTemplate(Serializable.PatchIdentifier identifier, Serializable.Mat mat) {
        if (addedTempList.size() > maxTemplateSize)
            return;
        Mat image = mat.toJavaCVMat();
        Mat descriptor = new Mat();
        KeyPoint keyPoints = new KeyPoint();
        sift.detectAndCompute(image, Mat.EMPTY, keyPoints, descriptor);
        addedTempList.add(new LogoTemplate(image, keyPoints, descriptor, identifier));

    }

    /**
     * Adds template to the dynamic list of logo templates.
     *
     * @param identifier The identifier of the patch, from which this template was extracted.
     * @param wholeFrame Image of the logo template
     * @param roi        Region where this logo template was detected
     */
    public void addTemplateByRect(Serializable.PatchIdentifier identifier, Serializable.Mat wholeFrame, Serializable.Rect roi) {
        if (addedTempList.size() > maxTemplateSize)
            return;
        Mat image = new Mat(wholeFrame.toJavaCVMat(), roi.toJavaCVRect());
        Mat descriptor = new Mat();
        KeyPoint keyPoints = new KeyPoint();
        sift.detectAndCompute(image, Mat.EMPTY, keyPoints, descriptor);
        addedTempList.add(new LogoTemplate(image, keyPoints, descriptor, identifier));

    }

    /**
     * Adds template to the dynamic list of logo templates.
     *
     * @param identifier        The identifier of the patch, from which this template was extracted.
     * @param extractedTemplate Image of the logo template
     */
    public void addTemplateBySubMat(Serializable.PatchIdentifier identifier, Serializable.Mat extractedTemplate) {
        if (addedTempList.size() > maxTemplateSize) {
            return;
        }

        Mat image = extractedTemplate.toJavaCVMat().clone();
        Mat descriptor = new Mat();
        KeyPoint keyPoints = new KeyPoint();
        sift.detectAndCompute(image, Mat.EMPTY, keyPoints, descriptor);
        addedTempList.add(new LogoTemplate(image, keyPoints, descriptor, identifier));
    }

    /**
     * For debug only. Returns the string representation of the content the logo template lists.
     *
     * @return String representation of the lists.
     * @see LogoTemplate#toString()
     */
    public String getTemplateInfo() {
        return "" + originalTemp + ", " + addedTempList;
    }
}
