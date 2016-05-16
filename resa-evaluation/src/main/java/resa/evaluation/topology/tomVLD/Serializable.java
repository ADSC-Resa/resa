package resa.evaluation.topology.tomVLD;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_features2d;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * This class provides kryo serialization for the JavaCV's Mat and Rect objects, so that Storm can wrap them in tuples.
 * Serializable.Mat - kryo serializable analog of opencv_core.Mat object.<p>
 * Serializable.Rect - kryo serializable analog of opencv_core.Rect object.<p>
 * Serializable.PatchIdentifier is also kryo serializable object,
 * which is used to identify each patch of the frame.<p>
 * <p>
 *
 * @author Nurlan Kanapin
 * @see Serializable.Mat
 * @see Serializable.Rect
 * @see Serializable.PatchIdentifier
 */
public class Serializable {

    /**
     * Kryo Serializable Mat class.
     * Essential fields are image data itself, rows and columns count and type of the data.
     */
    public static class Mat implements KryoSerializable, java.io.Serializable {
        private byte[] data;
        private int rows, cols, type;

        public int getRows() {
            return rows;
        }

        public int getCols() {
            return cols;
        }

        public int getType() {
            return type;
        }

        public Mat() {
        }

        /**
         * Creates new serializable Mat given its format and data.
         *
         * @param rows Number of rows in the Mat object
         * @param cols Number of columns in the Mat object
         * @param type OpenCV type of the data in the Mat object
         * @param data Byte data containing image.
         */
        public Mat(int rows, int cols, int type, byte[] data) {
            this.rows = rows;
            this.cols = cols;
            this.type = type;
            this.data = data;
        }

        /**
         * Creates new serializable Mat from opencv_core.Mat
         *
         * @param mat The opencv_core.Mat
         */
        public Mat(opencv_core.Mat mat) {
            if (!mat.isContinuous())
                mat = mat.clone();

            this.rows = mat.rows();
            this.cols = mat.cols();
            this.type = mat.type();
            int size = mat.arraySize();
            this.data = new byte[size];
//            mat.getByteBuffer().get(this.data);

            ByteBuffer bb = mat.getByteBuffer();
            bb.rewind();
            this.data = new byte[size];
            while (bb.hasRemaining())  // should happen only once
                bb.get(this.data);
        }

        /**
         * Creates new serializable Mat given its format and data.
         *
         * @param input Byte data containing image.
         */
        public Mat(byte[] input) {
            ByteArrayInputStream bis = new ByteArrayInputStream(input);
            ObjectInput in = null;
            try {
                in = new ObjectInputStream(bis);
                this.rows = in.readInt();
                this.cols = in.readInt();
                this.type = in.readInt();
                int size = in.readInt();
                this.data = new byte[size];
                int readed = 0;
                while (readed < size) {
                    readed += in.read(data, readed, size - readed);
                }
                //System.out.println("in: " + this.rows + "-" + this.cols + "-" + this.type + "-" + size + "-" + readed);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public byte[] toByteArray() {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = null;
            try {
                out = new ObjectOutputStream(bos);
                out.writeInt(this.rows);
                out.writeInt(this.cols);
                out.writeInt(this.type);
                out.writeInt(this.data.length);
                out.write(this.data);
                out.close();
                byte[] int_bytes = bos.toByteArray();
                bos.close();

                //System.out.println("out: " + this.rows + "-" + this.cols + "-" + this.type + "-" + this.data.length + "-" + int_bytes.length);
                return int_bytes;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        public static byte[] toByteArray(Serializable.Mat rawFrame, Serializable.Mat optFlow) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = null;
            try {
                out = new ObjectOutputStream(bos);

                out.writeInt(rawFrame.rows);
                out.writeInt(rawFrame.cols);
                out.writeInt(rawFrame.type);
                out.writeInt(rawFrame.data.length);
                out.write(rawFrame.data);

                out.writeInt(optFlow.rows);
                out.writeInt(optFlow.cols);
                out.writeInt(optFlow.type);
                out.writeInt(optFlow.data.length);
                out.write(optFlow.data);

                out.close();
                byte[] int_bytes = bos.toByteArray();
                bos.close();

                //System.out.println("out: " + this.rows + "-" + this.cols + "-" + this.type + "-" + this.data.length + "-" + int_bytes.length);
                return int_bytes;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        public static Serializable.Mat[] toSMat(byte[] input) {
            ByteArrayInputStream bis = new ByteArrayInputStream(input);
            ObjectInput in = null;
            Serializable.Mat rawFrame = new Serializable.Mat();
            Serializable.Mat optFlow = new Serializable.Mat();

            try {
                in = new ObjectInputStream(bis);
                rawFrame.rows = in.readInt();
                rawFrame.cols = in.readInt();
                rawFrame.type = in.readInt();
                int size = in.readInt();
                rawFrame.data = new byte[size];
                int readed = 0;
                while (readed < size) {
                    readed += in.read(rawFrame.data, readed, size - readed);
                }
                optFlow.rows = in.readInt();
                optFlow.cols = in.readInt();
                optFlow.type = in.readInt();
                size = in.readInt();
                optFlow.data = new byte[size];
                readed = 0;
                while (readed < size) {
                    readed += in.read(optFlow.data, readed, size - readed);
                }

                return new Serializable.Mat[]{rawFrame, optFlow};
                //System.out.println("in: " + this.rows + "-" + this.cols + "-" + this.type + "-" + size + "-" + readed);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }


        /**
         * @return Converts this Serializable Mat into JavaCV's Mat
         */
        public opencv_core.Mat toJavaCVMat() {
            return new opencv_core.Mat(rows, cols, type, new BytePointer(data));
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeInt(this.rows);
            output.writeInt(this.cols);
            output.writeInt(this.type);
            output.writeInt(this.data.length);
            output.writeBytes(this.data);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            this.rows = input.readInt();
            this.cols = input.readInt();
            this.type = input.readInt();
            int size = input.readInt();
            this.data = input.readBytes(size);
        }
    }

    /**
     * Kryo Serializable Rect class.
     */
    public static class Rect implements KryoSerializable, java.io.Serializable {
        /**
         * x, y, width, height - x and y coordinates of the left upper corner of the rectangle, its width and height
         */
        public int x, y, width, height;

        public Rect() {
        }

        public Rect(opencv_core.Rect rect) {
            x = rect.x();
            y = rect.y();
            width = rect.width();
            height = rect.height();
        }

        public Rect(int x, int y, int width, int height) {
            this.x = x;
            this.y = y;
            this.height = height;
            this.width = width;
        }

        public opencv_core.Rect toJavaCVRect() {
            return new opencv_core.Rect(x, y, width, height);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Rect rect = (Rect) o;

            if (height != rect.height) return false;
            if (width != rect.width) return false;
            if (x != rect.x) return false;
            if (y != rect.y) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = x;
            result = 31 * result + y;
            result = 31 * result + width;
            result = 31 * result + height;
            return result;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeInt(x);
            output.writeInt(y);
            output.writeInt(width);
            output.writeInt(height);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            x = input.readInt();
            y = input.readInt();
            width = input.readInt();
            height = input.readInt();
        }
    }

    /**
     * This is a serializable class used for patch identification. Each patch needs to be distinguished form others.
     * Each patch is uniquely identified by the id of its frame and by the rectangle it corresponds to.
     */
    public static class PatchIdentifier implements KryoSerializable, java.io.Serializable {
        /**
         * Frame id of this patch
         */
        public int frameId;
        /**
         * Rectangle or Region of Interest of this patch.
         */
        public Rect roi;

        public PatchIdentifier() {
        }

        /**
         * Creates PatchIdentifier with given frame id and rectangle.
         *
         * @param frameId
         * @param roi
         */
        public PatchIdentifier(int frameId, Rect roi) {
            this.roi = roi;
            this.frameId = frameId;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeInt(frameId);
            output.writeInt(roi.x);
            output.writeInt(roi.y);
            output.writeInt(roi.width);
            output.writeInt(roi.height);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            frameId = input.readInt();
            int x = input.readInt();
            int y = input.readInt();
            int width = input.readInt();
            int height = input.readInt();
            roi = new Rect(x, y, width, height);
        }

        /**
         * String representation of this patch identifier.
         *
         * @return the string in the format N%04d@%04d@%04d@%04d@%04d if roi is not null, and N%04d@null otherwise.
         */
        public String toString() {
            if (roi != null)
                return String.format("N%04d@%04d@%04d@%04d@%04d", frameId, roi.x, roi.y, roi.x + roi.width, roi.y + roi.height);
            return String.format("N%04d@null", frameId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PatchIdentifier that = (PatchIdentifier) o;

            if (frameId != that.frameId) return false;
            if (roi != null ? !roi.equals(that.roi) : that.roi != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = frameId;
            result = 31 * result + (roi != null ? roi.hashCode() : 0);
            return result;
        }
    }

    public static class CvPoint2D32f implements KryoSerializable, java.io.Serializable {
        float x;
        float y;

        public CvPoint2D32f() {
        }

        public CvPoint2D32f(opencv_core.CvPoint2D32f p) {
            this.x = p.x();
            this.y = p.y();
        }

        public CvPoint2D32f(CvPoint2D32f p) {
            this.x = p.x();
            this.y = p.y();
        }

        public opencv_core.CvPoint2D32f toJavaCvPoint2D32f() {
            return new opencv_core.CvPoint2D32f().x(this.x).y(this.y);
        }

        public float x() {
            return this.x;
        }

        public float y() {
            return this.y;
        }

        public void x(float x) {
            this.x = x;
        }

        public void y(float y) {
            this.y = y;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeFloat(this.x);
            output.writeFloat(this.y);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            this.x = input.readFloat();
            this.y = input.readFloat();
        }
    }

    /**
     * This is a serializable class used for patch identification. Each patch needs to be distinguished form others.
     * Each patch is uniquely identified by the id of its frame and by the rectangle it corresponds to.
     */
    public static class PatchIdentifierMat implements java.io.Serializable {

        public PatchIdentifier identifier;
        public Mat sMat;


        /**
         * Creates PatchIdentifier with given frame id and rectangle.
         * @param frameId
         * @param roi
         * @param sMat
         */
        public PatchIdentifierMat(int frameId, Rect roi, Mat sMat) {
            this.identifier = new PatchIdentifier(frameId, roi);
            this.sMat = sMat;
        }

        /**
         * String representation of this patch identifier.
         *
         * @return the string in the format N%04d@%04d@%04d@%04d@%04d if roi is not null, and N%04d@null otherwise.
         */
        public String toString() {
            return this.identifier.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PatchIdentifierMat that = (PatchIdentifierMat) o;
            return this.identifier.equals(that.identifier);
        }

        @Override
        public int hashCode() {
            return this.identifier.hashCode();
        }
    }

    public static class KeyPoint implements KryoSerializable, java.io.Serializable{
        float x;
        float y;
        float size;
        float angle;
        float response;
        int octave;
        int classId;

        public KeyPoint() {
        }

        public KeyPoint(opencv_features2d.KeyPoint p) {
            this.x = p.pt().x();
            this.y = p.pt().y();
            this.size = p.size();
            this.angle = p.angle();
            this.response = p.response();
            this.octave = p.octave();
            this.classId = p.class_id();
        }

        public KeyPoint(KeyPoint p) {
            this.x = p.x;
            this.y = p.y;
            this.size = p.size;
            this.angle = p.angle;
            this.response = p.response;
            this.octave = p.octave;
            this.classId = p.classId;
        }

        public opencv_features2d.KeyPoint toJavaCvFeatures2dKeyPoint() {
//            opencv_features2d.KeyPoint retVal = new opencv_features2d.KeyPoint();
//            retVal.pt(new opencv_core.Point2f(x, y));
//            retVal.size(size);
//            retVal.angle(angle);
//            retVal.response(response);
//            retVal.octave(octave);
//            retVal.class_id(classId);
//            return retVal;

            return new opencv_features2d.KeyPoint().pt(new opencv_core.Point2f(x, y))
                    .size(size).angle(angle).response(response).octave(octave).class_id(classId);
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeFloat(this.x);
            output.writeFloat(this.y);
            output.writeFloat(this.size);
            output.writeFloat(this.angle);
            output.writeFloat(this.response);
            output.writeInt(this.octave);
            output.writeInt(this.classId);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            this.x = input.readFloat();
            this.y = input.readFloat();
            this.size = input.readFloat();
            this.angle = input.readFloat();
            this.response = input.readFloat();
            this.octave = input.readInt();
            this.classId = input.readInt();
        }

    }
}
