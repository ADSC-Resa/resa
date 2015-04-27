package resa.evaluation.topology.tomVLD;

import org.bytedeco.javacpp.opencv_core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * This class provides static geometric methods for refining the results given by RANSAC algorithm
 * {@link resa.evaluation.topology.tomVLD.RobustMatcher#matchImages(opencv_core.Mat, opencv_core.Mat,
 * org.bytedeco.javacpp.opencv_features2d.KeyPoint, opencv_core.Mat, opencv_core.Mat,
 * org.bytedeco.javacpp.opencv_features2d.KeyPoint, opencv_core.Rect)}
 */
public class Util {

    /** Draws rectangle on given image matrix with color given as a scalar */
    public static void drawRectOnMat(opencv_core.Rect r, opencv_core.Mat finalImage, opencv_core.CvScalar scalar) {
        opencv_core.Scalar color = new opencv_core.Scalar(scalar);
        opencv_core.Point A = new opencv_core.Point(r.x(), r.y()), B = new opencv_core.Point(r.x() + r.width(), r.y()),
                C = new opencv_core.Point(r.x() + r.width(), r.y()+ r.height()), D = new opencv_core.Point(r.x(), r.y() + r.height());
        opencv_core.line(finalImage, A, B, color, 4, 4, 0);
        opencv_core.line(finalImage, B, C, color, 4, 4, 0);
        opencv_core.line(finalImage, C, D, color, 4, 4, 0);
        opencv_core.line(finalImage, D, A, color, 4, 4, 0);
    }

    /**
     *  Draws quadrilateral on given image matrix with color given as a scalar
     * @param Q 4x2 array with coordinates of the quadrilateral
     * @param finalImage image matrix on which to draw an image
     * @param scalar the color
     */
    public static void drawQonMat(double [][] Q, opencv_core.Mat finalImage, opencv_core.CvScalar scalar) {
        opencv_core.Scalar color = new opencv_core.Scalar(scalar);
        for (int i = 0; i < 4; i++) {
            opencv_core.line(finalImage, new opencv_core.Point((int) Q[i][0], (int) Q[i][1]),
                    new opencv_core.Point((int) Q[(i + 1) % 4][0], (int) Q[(i + 1) % 4][1]), color, 4, 4, 0);

        }
    }

    /** epsilon for checking whether doubles are positive or negative {@link #CCW(double[], double[], double[])} */
    final static double eps = 1e-2;

    /**
     * Is the quadrilateral convex?
     * @param corners - the 4x2 array of doubles - the coordinates of corners given in counter clockwise order
     * @return true if argument is a convex quadrilateral and false otherwise.
     */
    public static boolean isConvex(double [][] corners) {
        assert corners.length == 4;
        for (int i = 0 ; i < 4 ; i ++)
            assert corners[i].length == 2;
        for (int i = 0 ; i < 4 ; i ++) { // vprev * vnext > 0
            int prev = (i - 1 + 4) % 4, next = ( i + 1 ) % 4;
            if (CCW(corners[prev], corners[i], corners[next]))
                return false;
        }
        return true;
    }

    /**
     * Are points a, b and c are in clockwise order?
     * @param a
     * @param b
     * @param c
     * @return true, if a, b, c are in CCW order
     */
    public static boolean CCW(double[] a, double[] b, double[] c) {
        double x1 = a[0] - b[0], y1 = a[1] - b[1];
        double x2 = c[0] - b[0], y2 = c[1] - b[1];
        return x1 * y2 - x2 * y1 > eps;
    }

    /**
     * Returns the area of quadrilateral.
     * @param p corners of quadrilateral given in CCW order
     * @return area
     */
    public static double area(double [][] p) {
        double res = 0.0;
        for (int i = 0 ; i < 4 ; i ++) {
            res += p[i][0] * p[(i+1)%4][1] - p[(i+1)%4][0] * p[i][1];
        }
        return Math.abs(res);
    }

    /**
     * Performs testing of quadrilateral obtained by transformation of the corners of logo template onto frame, after
     * the logo has been detected.
     * 1. Checks if it's not smaller than 10x10 pixel square <p>
     * 2. Checks if it's not too large and does fall outside the patch <p>
     * 3. Checks if it's not too flattened <p>
     * 4. Checks that it is a convex quadrilateral.
     * @param scene_corners corners of quadrilateral
     * @param roi the patch coordinates where this logo
     * @return true if quadrilateral satisfies all requirements, false otherwise.
     */
    public static boolean checkQuadrilateral(double [][] scene_corners, opencv_core.Rect roi) {
        double xMax = 0.0, xMin = 1e100;
        double yMax = 0.0, yMin = 1e100;
        for (int i = 0 ; i < 4 ; i ++) {
            xMax = Math.max(xMax, scene_corners[i][0]);
            xMin = Math.min(xMin, scene_corners[i][0]);
            yMax = Math.max(yMax, scene_corners[i][1]);
            yMin = Math.min(yMin, scene_corners[i][1]);
        }

        // TODO: update 07/29. If opposite sides are too far from being a rectangle
        /*
        double [] sides = new double [4];
        for (int i = 0 ; i < 4 ; i ++) {
            sides[i] = (scene_corners[i][0] - scene_corners[(i+1)%4][0])*(scene_corners[i][0] - scene_corners[(i+1)%4][0]);
        }
        for (int i = 0 ; i < 2 ; i ++)
            if (sides[i] / sides[i + 2] > 3.0 || sides[i] / sides[i + 2] < 1.0 / 3) {
                System.out.println("Too squeezed");
                return false;
            }

        */

        // TODO: adjust these constants.
        if (xMax - xMin < 10 || yMax - yMin < 10) {
            if (Debug.logoDetectionDebugOutput)
                System.out.println("Of too small resolution");
            return false;
        }
        double wx = xMax - xMin, hy = yMax - yMin;

        // TODO: update 07/21. If the quadrilateral is too large with respect to roi, it is counted as bad
        if (wx > 2 * roi.width() || hy > 2 * roi.height()) {
            if (Debug.logoDetectionDebugOutput)
                System.out.println("Too large");
            return false;
        }


        // TODO: update 07/21 If quadrilateral is too 'flattened' return false (Area < C * Bounding_box_area), C = 1/2
        if (Util.area(scene_corners) < .5 * (xMax - xMin) * (yMax - yMin)) {
            if (Debug.logoDetectionDebugOutput)
                System.out.println("Of too small area");
            return false;
        }
        if (!Util.isConvex(scene_corners)) {
            if (Debug.logoDetectionDebugOutput)
                System.out.println("not a convex");
            return false;
        }
        return true;
    }

    /**
     * Given list of points find the rectangle of minimal area which includes at least accuracy*100% points. O(n^3)
     * @param list - list of 2D points
     * @param accuracy - the ratio
     * @return the rectangle enclosing points.
     */
    public static opencv_core.Rect bestBoundingBoxFast(ArrayList<opencv_core.Point2f> list, double accuracy) {
        if (accuracy < 0.0 || accuracy > 1.0) {
            System.err.println("Not valid accuracy [0.0, 1.0]");
            accuracy = .9;
        }
        int N = list.size(), n = (int)Math.ceil(N * accuracy);
        opencv_core.Rect best = new opencv_core.Rect(0, 0, 1 << 15, 1 << 15); // 'infinite' rectangle

        ArrayList<opencv_core.Point2f> xSorted = (ArrayList<opencv_core.Point2f>)list.clone();
        ArrayList<opencv_core.Point2f> ySorted = (ArrayList<opencv_core.Point2f>)list.clone();

        Collections.sort(xSorted, new Comparator<opencv_core.Point2f>() {
            public int compare(opencv_core.Point2f o1, opencv_core.Point2f o2) {
                if (o1.x() < o2.x()) return -1;
                if (o1.x() > o2.x()) return 1;
                if (o1.y() < o2.y()) return -1;
                if (o1.y() > o2.y()) return 1;
                return 0;
            }
        });
        Collections.sort(ySorted, new Comparator<opencv_core.Point2f>() {
            public int compare(opencv_core.Point2f o1, opencv_core.Point2f o2) {
                if (o1.y() < o2.y()) return -1;
                if (o1.y() > o2.y()) return 1;
                if (o1.x() > o2.x()) return -1;
                if (o1.x() < o2.x()) return 1;
                return 0;
            }
        });

        for (int i = 0 ; i < N ; i ++) {
            for (int j = 0 ; j < N ; j ++) {
    			/*
    			int cur = 0, pup = N - 1, pright = i;
    			while (cur < n && pright < N) {
    				if (xSorted.get(pright).y() >= ySorted.get(j).y())
    					cur ++;
    				pright ++;
    			}
    			if (cur < n) break;// ????
    			double xMin = xSorted.get(i).x(), xMax = xSorted.get(pright).x(),
						yMin = ySorted.get(j).y(), yMax = ySorted.get(pup).y();
				*/
                int cur = 0;
                double xMin = xSorted.get(i).x(),
                        yMin = ySorted.get(j).y();
                for (int pup = N - 1, pright = i - 1; pup >= j ;  ) {
                    while ( pright + 1 < N &&  (cur < n || (pright < 0 || xSorted.get(pright).x() == xSorted.get(pright + 1).x()) )  ) {
                        if (xSorted.get(pright + 1).y() >= ySorted.get(j).y() && xSorted.get(pright + 1).y() <= ySorted.get(pup).y())
                            cur ++;
                        pright = pright + 1;
                    }

                    if (cur < n) break;// ????

                    double xMax = xSorted.get(pright).x();
                    double yMax = ySorted.get(pup).y();
                    if ((xMax - xMin) * (yMax - yMin) < best.area()) {
                        best = new opencv_core.Rect((int)xMin, (int)yMin, (int)(xMax - xMin), (int)(yMax - yMin));
                    }
                    if ( ySorted.get(pup).x() >= xMin && ySorted.get(pup).x() <= xMax) {
                        cur --;
                    }

                    pup --;
                }
            }
        }
        return best;
    }
}
