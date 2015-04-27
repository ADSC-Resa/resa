package resa.evaluation.topology.tomVLD;

import org.bytedeco.javacpp.opencv_core;

/**
 * The class wrapping the output of the topology - a frame with a list of rectangles found on it.
 * frames are ordered by their frame id.
 */
public class StreamFrame implements Comparable<StreamFrame> {
    final public int frameId;
    final public opencv_core.Mat image;

    /**
     * creates a StreamFrame with given id, image matrix and list of rectangles corresponding to the detected logos.
     * @param frameId
     * @param image
     */
    public StreamFrame(int frameId, opencv_core.Mat image) {
        this.frameId = frameId;
        this.image = image;
    }

    @Override
    public int compareTo(StreamFrame o) {
        return frameId - o.frameId;
    }
}
