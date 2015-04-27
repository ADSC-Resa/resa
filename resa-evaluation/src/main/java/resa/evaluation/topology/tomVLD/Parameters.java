package resa.evaluation.topology.tomVLD;

/**
 * Parameters for detection.
 */
public class Parameters {
    public SIFTParameters getSiftParameters() {
        return siftParameters;
    }

    public void setSiftParameters(SIFTParameters siftParameters) {
        this.siftParameters = siftParameters;
    }

    public RANSACParameters getRansacParameters() {
        return ransacParameters;
    }

    public void setRansacParameters(RANSACParameters ransacParameters) {
        this.ransacParameters = ransacParameters;
    }

    public MatchingParameters getMatchingParameters() {
        return matchingParameters;
    }

    public void setMatchingParameters(MatchingParameters matchingParameters) {
        this.matchingParameters = matchingParameters;
    }

    public WindowParameters getWindowParameters() {
        return windowParameters;
    }

    public void setWindowParameters(WindowParameters windowParameters) {
        this.windowParameters = windowParameters;
    }


    public static class SIFTParameters {
        private double contrastThreshold, edgeThreshold, sigma;
        public SIFTParameters() {
            contrastThreshold = 0.04;
            edgeThreshold = 10;
            sigma = 2.0;
        }
        public SIFTParameters withContrastThreshold(double threshold) {
            this.contrastThreshold = threshold;
            return this;
        }
        public SIFTParameters withEdgeThreshold(double threshold) {
            this.edgeThreshold = threshold;
            return this;
        }
        public SIFTParameters withSigma(double sigma) {
            this.sigma = sigma;
            return this;
        }

        public double getContrastThreshold() {
            return contrastThreshold;
        }

        public void setContrastThreshold(double contrastThreshold) {
            this.contrastThreshold = contrastThreshold;
        }

        public double getEdgeThreshold() {
            return edgeThreshold;
        }

        public void setEdgeThreshold(double edgeThreshold) {
            this.edgeThreshold = edgeThreshold;
        }

        public double getSigma() {
            return sigma;
        }

        public void setSigma(double sigma) {
            this.sigma = sigma;
        }
    }
    public static class RANSACParameters {
        private double reprojectionThreshold;
        public RANSACParameters() {
            reprojectionThreshold = 1.0;
        }
        public RANSACParameters withReprojectionThreshold(double threshold) {
            this.reprojectionThreshold = threshold;
            return this;
        }

        public double getReprojectionThreshold() {
            return reprojectionThreshold;
        }

        public void setReprojectionThreshold(double reprojectionThreshold) {
            this.reprojectionThreshold = reprojectionThreshold;
        }
    }
    public static class MatchingParameters {
        private int minimalNumberOfMatches;
        private double ratioOfDistances;
        private int minimalResolution;
        private double boxAccuracy;
        public MatchingParameters() {
            minimalNumberOfMatches = 8;
            ratioOfDistances = 0.8;
            minimalResolution = 5; //pixels
            boxAccuracy = 0.9;
        }

        public MatchingParameters withMinimalNumberOfMatches(int minimalNumberOfMatches) {
            this.minimalNumberOfMatches = minimalNumberOfMatches;
            return this;
        }

        public int getMinimalNumberOfMatches() {
            return minimalNumberOfMatches;
        }

        public void setMinimalNumberOfMatches(int minimalNumberOfMatches) {
            this.minimalNumberOfMatches = minimalNumberOfMatches;
        }

        public double getRatioOfDistances() {
            return ratioOfDistances;
        }

        public void setRatioOfDistances(double ratioOfDistances) {
            this.ratioOfDistances = ratioOfDistances;
        }

        public int getMinimalResolution() {
            return minimalResolution;
        }

        public void setMinimalResolution(int minimalResolution) {
            this.minimalResolution = minimalResolution;
        }

        public double getBoxAccuracy() {
            return boxAccuracy;
        }

        public void setBoxAccuracy(double boxAccuracy) {
            this.boxAccuracy = boxAccuracy;
        }
    }
    public static class WindowParameters {
        private double [] windowSizes;
        private double xStep, yStep;
        public WindowParameters() {
            windowSizes = new double[]{.25}; // by default 1/4 of the frame
            xStep = .33; // in terms of window size
            yStep = .33;
        }

        public double[] getWindowSizes() {
            return windowSizes;
        }

        public void setWindowSizes(double[] windowSizes) {
            this.windowSizes = windowSizes;
        }

        public double getyStep() {
            return yStep;
        }

        public void setyStep(double yStep) {
            this.yStep = yStep;
        }

        public double getxStep() {
            return xStep;
        }

        public void setxStep(double xStep) {
            this.xStep = xStep;
        }
    }


    private SIFTParameters siftParameters;
    private RANSACParameters ransacParameters;
    private MatchingParameters matchingParameters;
    private WindowParameters windowParameters;

    public Parameters() {
        siftParameters = new SIFTParameters();
        ransacParameters = new RANSACParameters();
        matchingParameters = new MatchingParameters();
        windowParameters = new WindowParameters();
    }
    public Parameters withSIFTParameters(SIFTParameters siftParameters) {
        this.siftParameters = siftParameters;
        return this;
    }
    public Parameters withRANSACParameters(RANSACParameters ransacParameters) {
        this.ransacParameters = ransacParameters;
        return this;
    }
    public Parameters withMatchingParameters(MatchingParameters matchingParameters) {
        this.matchingParameters = matchingParameters;
        return this;
    }
    public Parameters withWindowParameters(WindowParameters windowParameters) {
        this.windowParameters = windowParameters;
        return this;
    }

}
