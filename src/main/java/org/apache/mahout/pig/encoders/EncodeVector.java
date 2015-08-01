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

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.pig.ImpossibleStateError;
import org.apache.mahout.pig.PigVector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Encodes a tuple as a vector using the hashed vector representation.
 */
public class EncodeVector extends EvalFunc<DataByteArray> {
    private int dimension;
    private EncodingSpec spec;
    private Map<String, ArgumentEncoder> encoderMap;
    private final FeatureVectorEncoder constantEncoder = new ConstantValueEncoder("- bias -");
    private int minimumTupleSize;

    /**
     * This class requires that a formula and a schema be provided in order to specify how
     * to encode tuples.  The formula should be like an R formula such as is used by lm or glm.
     * <p/>
     * The formula can look like this
     * <p/>
     * x + y*z + 1
     * <p/>
     * This indicates that x, the interaction between x and y and a constant offset should be encoded.
     * <p/>
     * The schema looks generally like a pig schema except that only the following primitive types are recognized:
     * <p/>
     * numeric
     * word
     * text
     * text(Analyzer)
     *
     * @param dimension Number of elements in the resulting encoded vectors
     * @param formula   The R-like formula that describes the data to encode
     * @param schema    A schema description for the arguments that will be given to the function later.
     */
    public EncodeVector(String dimension, String formula, String schema) {
        this.dimension = Integer.parseInt(dimension);
        encoderMap = Schema.parse(schema);
        minimumTupleSize = 0;
        for (ArgumentEncoder encoder : encoderMap.values()) {
            minimumTupleSize = Math.max(minimumTupleSize, encoder.getPosition() + 1);
        }
        spec = Formula.parse(formula);
    }

    public DataByteArray exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.size() < minimumTupleSize) {
            throw new IllegalArgumentException("Tuple doesn't have at least " + minimumTupleSize + " elements");
        } else {
            Vector r = new RandomAccessSparseVector(dimension);
            for (Set<String> variables : spec.getVariables()) {
                if (variables.size() > 1) {
                    throw new UnsupportedOperationException("Can't encode interactions yet");
                }
                if (variables.size() == 0) {
                    throw new ImpossibleStateError("No variables!");
                }
                final String var = variables.iterator().next();
                if ("1".equals(var)) {
                    constantEncoder.addToVector((byte[]) null, r);
                } else {
                    ArgumentEncoder encoder = encoderMap.get(var);
                    encoder.getEncoder().addToVector(input.get(encoder.getPosition()).toString(), r);
                }
            }
            return PigVector.toBytes(r);
        }
    }
}
