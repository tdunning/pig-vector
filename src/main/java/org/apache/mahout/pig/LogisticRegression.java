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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.classifier.sgd.PolymorphicWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

import java.io.*;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Trains a logistic regression model entirely in memory using the simplest learning algorithm from Mahout.
 * <p/>
 * A number of options can be supplied in a configuration string passed to the constructor.  These options
 * are of the form name=value and options are separated by commas.  Whitespace around option names and around
 * values is not significant.  Spaces inside values are preserved.
 * <p/>
 * The model to be trained can be read from a file in order to allow learning to continue at a previous
 * stopping point or the particulars of a new model to be trained from scratch can be specified using the
 * options.  Training data can be held in-memory or written to a temporary file.
 * <p/>
 * The following options can be used to load a pre-existing model:
 * <p/>
 * <ul>
 * <li>model</li>
 * <li>categories</li>
 * </ul>
 * <p/>
 * The following options can be used with a newly created model:
 * <p/>
 * <ul>
 * <li>categories - specifies a list of values that the target variable can take on.  This list should be space
 * separated and given in the same order as when the model is later used.  (required)</li>
 * <li>features - the number of elements in the feature vectors to be given to the learning algorithm.   (required)</li>
 * <li>decayExponent - specifies how quickly the learning rate will decay.  Optional with default value of 0.5.
 * Note that per-term annealing still has effect.</li>
 * <li>lambda - specifies how much regularization constant is used.  Optional with default of 1e-5.</li>
 * <li>stepOffset - slows down the decay of the learning rate at first if set greater than zero.  Default is 10</li>
 * <li>learningRate - initial value of the learning rate.  Default is 1.</li>
 * </ul>
 * <p/>
 * The following options can be used with any model:
 * <p/>
 * <ul>
 * <li>inMemory - if "true" then training examples are kept in-memory and used in a random order.  If "false" then
 * a temporary file is used to hold training examples and the order they are used is fixed by the order they are
 * given to this UDF.  The default is "true".</li>
 * <li>iterations - the number of iterations through the training data that are to be taken.</li>
 * </ul>
 */
public class LogisticRegression extends EvalFunc<DataByteArray> implements Accumulator<DataByteArray> {
    private List<String> categories;
    OnlineLogisticRegression model;
    List<Example> trainingData = Lists.newArrayList();
    private int iterations;
    private boolean inMemory;
    private File tmpFile;

    public LogisticRegression(String modelParams) throws IOException {
        Splitter onComma = Splitter.on(",").trimResults().omitEmptyStrings();
        Splitter onEquals = Splitter.on("=").trimResults();
        Splitter onSpaces = Splitter.on(" ");
        Joiner withSpaces = Joiner.on(" ");

        Map<String, String> options = Maps.newHashMap();

        for (String option : onComma.split(modelParams)) {
            List<String> values = Lists.newArrayList(onEquals.split(option));
            options.put(values.get(0), values.get(1));
        }

        if (options.containsKey("model")) {
            if (options.containsKey("categories")) {
                categories = Lists.newArrayList(onSpaces.split(options.get("categories")));
                Configuration conf = UDFContext.getUDFContext().getJobConf();
                model = PolymorphicWritable.read(FileSystem.get(conf).open(new Path(options.get("model"))), OnlineLogisticRegression.class);
                options.remove("model");
                options.remove(("categories"));
            } else {
                throw new BadClassifierSpecException("Must specify \"categories\" if pre-existing model is used");
            }
        } else {
            if (options.containsKey("categories") && options.containsKey("features")) {
                categories = Lists.newArrayList(onSpaces.split(options.get("categories")));
                if (categories.size() < 2) {
                    throw new BadClassifierSpecException("Must have more than one target category.  Remember that categories is a space separated list");
                }
                model = new OnlineLogisticRegression(categories.size(), Integer.parseInt(options.get("features")), new L1());
                options.remove("categories");
                options.remove("features");
            } else {
                throw new BadClassifierSpecException("Must specify previous model location using \"file\" or supply \"categories\" and \"features\"");
            }

            if (options.containsKey("decayExponent")) {
                model.decayExponent(Double.parseDouble(options.get("decayExponent")));
                options.remove("decayExponent");
            }

            if (options.containsKey("lambda")) {
                model.lambda(Double.parseDouble(options.get("lambda")));
                options.remove("lambda");
            }

            if (options.containsKey("stepOffset")) {
                model.stepOffset(Integer.parseInt(options.get("stepOffset")));
                options.remove("stepOffset");
            }

            if (options.containsKey("learningRate")) {
                model.learningRate(Double.parseDouble(options.get("learningRate")));
                options.remove("learningRate");
            }
        }

        iterations = options.containsKey("iterations") ? Integer.parseInt(options.get("iterations")) : 1;
        options.remove("iterations");

        inMemory = options.containsKey("inMemory") ? Boolean.parseBoolean(options.get("inMemory")) : true;
        options.remove("inMemory");

        if (options.size() > 0) {
            throw new BadClassifierSpecException("Extra options supplied: " + withSpaces.join(options.keySet()));
        }

        if (!inMemory) {
            tmpFile = Files.createTempFile("trainingData", "tmp").toFile();
            tmpFile.deleteOnExit();
        }
    }

    @Override
    public DataByteArray exec(Tuple input) throws IOException {
        addBagOfData((DataBag) input.get(0));
        return getValue();
    }

    /**
     * Pass tuples to the learning algorithm.  Each tuple should have two fields.  The first
     * fields should correspond to one of the categories for the model and the second should
     * be the encoded features for the training example.
     *
     * @param example A tuple containing a single field, which is a bag.  The bag will contain the set
     *                of training examples being passed to the learning algorithm in this iteration.  Not all
     *                training examples will be passed at once.
     */
    public void accumulate(Tuple example) throws IOException {
        if (example.size() != 1) {
            throw new IllegalArgumentException("Input to training algorithm should be a single bag containing tuples each with target and vector");
        }
        addBagOfData((DataBag) example.get(0));
    }

    private void addBagOfData(DataBag data) throws IOException {
        if (inMemory) {
            for (Tuple input : data) {
                trainingData.add(new Example(categories.indexOf(input.get(0)), PigVector.fromBytes((DataByteArray) input.get(1))));
            }
        } else {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpFile));
            try {
                for (Tuple input : data) {
                    out.writeInt(categories.indexOf(input.get(0)));
                    PolymorphicWritable.write(out, new VectorWritable(PigVector.fromBytes((DataByteArray) input.get(1))));
                }
            } finally {
                out.close();
            }
        }
    }

    /**
     * Called when all tuples from current key have been passed to accumulate.  This is where the
     * actual training occurs.  We can't do it earlier unless iterations = 1 which is an unusual
     * case.
     *
     * @return the trained model.
     */
    public DataByteArray getValue() {
        for (int i = 0; i < iterations; i++) {
            for (Example example : readInput()) {
                model.train(example.getTarget(), example.getFeatures());
            }
        }

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            PolymorphicWritable.write(out, new Classifier(categories, model));
            out.close();

            return new DataByteArray(baos.toByteArray());
        } catch (IOException e) {
            // should never happen
            throw new ImpossibleStateError("Can't put results into tuple", e);
        }
    }

    /**
     * Called after getValue() to prepare processing for next key.
     */
    public void cleanup() {
        if (tmpFile != null) {
            tmpFile.delete();
        }
    }

    public int getIterations() {
        return iterations;
    }

    public boolean isInMemory() {
        return inMemory;
    }

    public OnlineLogisticRegression getModel() {
        return model;
    }

    private Iterable<Example> readInput() {
        if (inMemory) {
            return new Iterable<Example>() {
                public Iterator<Example> iterator() {
                    return new AbstractIterator<Example>() {
                        int remainingExamples = trainingData.size();
                        Random gen = new Random();

                        @Override
                        protected Example computeNext() {
                            if (remainingExamples > 0) {
                                remainingExamples--;
                                return trainingData.get(gen.nextInt(trainingData.size()));
                            } else {
                                return endOfData();
                            }
                        }
                    };
                }
            };
        } else {
            return new Iterable<Example>() {
                public Iterator<Example> iterator() {
                    try {
                        return new AbstractIterator<Example>() {
                            DataInputStream in = new DataInputStream(new FileInputStream(tmpFile));

                            @Override
                            protected Example computeNext() {
                                int target;
                                try {
                                    target = in.readInt();
                                } catch (EOFException e) {
                                    Closeables.closeQuietly(in);
                                    return endOfData();
                                } catch (IOException e) {
                                    Closeables.closeQuietly(in);
                                    throw new TrainingDataException("Error reading training data", e);
                                }
                                try {
                                    return new Example(target, PolymorphicWritable.read(in, VectorWritable.class));
                                } catch (EOFException e) {
                                    Closeables.closeQuietly(in);
                                    throw new TrainingDataException("Premature EOF while reading training data", e);
                                } catch (IOException e) {
                                    Closeables.closeQuietly(in);
                                    throw new TrainingDataException("Error reading training data", e);
                                }
                            }
                        };
                    } catch (FileNotFoundException e) {
                        throw new TrainingDataException("Could not training data file", e);
                    }
                }
            };
        }
    }

    private static class Example {
        int target;
        Vector features;

        public Example(int target, Vector v) {
            this.target = target;
            this.features = v;
        }

        public Example(int target, VectorWritable v) {
            this(target, v.get());
        }

        public int getTarget() {
            return target;
        }

        public Vector getFeatures() {
            return features;
        }
    }

    private static class TrainingDataException extends RuntimeException {
        public TrainingDataException(String msg, Throwable e) {
            super(msg, e);
        }
    }
}
