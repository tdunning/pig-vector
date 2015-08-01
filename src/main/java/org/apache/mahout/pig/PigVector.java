/*
 * Copyright 2014 Ted Dunning
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
