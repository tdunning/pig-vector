package org.apache.mahout.pig;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.pig.data.DataByteArray;

import java.io.*;

/**
 * Wraps a pig tuple around a Mahout vector.
 */
public class PigVector {
    public static DataByteArray toBytes(Vector v1) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            new VectorWritable(v1).write(dos);
            dos.close();
        } catch (IOException e) {
            throw new ImpossibleStateError("Can't have IO error on BAOS", e);
        }
        return new DataByteArray(baos.toByteArray());
    }

    public static Vector fromBytes(DataByteArray data) {
        try {
            VectorWritable r = new VectorWritable();
            r.readFields(new DataInputStream(new ByteArrayInputStream(data.get())));
            return r.get();
        } catch (IOException e) {
            throw new ImpossibleStateError("Can't have error in BAIS", e);
        }
    }
}
