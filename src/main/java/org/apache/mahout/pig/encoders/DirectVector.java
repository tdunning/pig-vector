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

package org.apache.mahout.pig.encoders;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.pig.PigVector;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Encodes a set of numeric fields into distinct elements of a vector.
 * <p/>
 * No hashing is done and no dictionaries are used so we have exactly as many elements
 * in the encoded vector as there are in the schema provided to the constructor of the
 * encoder.
 */
public class DirectVector extends EvalFunc<DataByteArray> {
    private final Splitter onCommas = Splitter.on(",").trimResults();
    private final Iterable<String> fieldNames;
    private final int dimension;

    /**
     * This class requires that a schema be provided in order to specify how
     * to encode tuples.  The formula should be a list of field names separated by
     * commas.
     *
     * @param schema A schema description for the arguments that will be given to the function later.
     */
    public DirectVector(String schema) {
        fieldNames = onCommas.split(schema);
        this.dimension = Iterables.size(fieldNames);
    }

    public DataByteArray exec(Tuple input) throws IOException {
        if (input == null || input.size() != dimension) {
            throw new IllegalArgumentException("Tuple doesn't have " + dimension + " elements");
        } else {
            Vector r = new DenseVector(dimension);
            for (int i = 0; i < dimension; i++) {
                final Object v = input.get(i);
                if (v != null) {
                    r.set(i, DataType.toDouble(v));
                } else {
                    r.set(i, Double.NaN);
                }
            }
            return PigVector.toBytes(r);
        }
    }

}
