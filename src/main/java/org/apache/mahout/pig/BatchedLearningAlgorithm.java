package org.apache.mahout.pig;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.classifier.sgd.PolymorphicWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.pig.annotations.Stable;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Stable
public class BatchedLearningAlgorithm extends EvalFunc<Tuple> implements Accumulator<Tuple> {
    private List<String> categories;
    OnlineLogisticRegression model;
    List<Vector> trainingData = Lists.newArrayList();
    List<Integer> targetValues = Lists.newArrayList();
    private int iterations;

    public BatchedLearningAlgorithm(String modelClass, String modelParams) throws IOException {
        Splitter onComma = Splitter.on(",").trimResults().omitEmptyStrings();
        Splitter onEquals = Splitter.on("=").trimResults();
        Splitter onSpaces = Splitter.on("\\s");
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
        if (options.size() > 0) {
            throw new BadClassifierSpecException("Extra options supplied: " + withSpaces.join(options.keySet()));
        }
    }

    /**
     * Train the model
     *
     * @param input the Tuple to be processed.
     * @return result, of type T.
     * @throws java.io.IOException
     */
    @Override
    public Tuple exec(Tuple input) throws IOException {
        throw new ExecException("Learning algorithm should only be called as an accumulator");
    }

    /**
     * Pass tuples to the learning algorithm.  Each tuple should have two fields.  The first
     * fields should correspond to one of the categories for the model and the second should
     * be the encoded features for the training example.
     *
     * @param b A tuple containing a single field, which is a bag.  The bag will contain the set
     *          of training examples being passed to the learning algorithm in this iteration.  Not all
     *          training examples will be passed at once.
     */
    public void accumulate(Tuple b) throws IOException {
        DataBag data = (DataBag) b.get(0);
        for (Tuple input : data) {
            targetValues.add(((Number) input.get(0)).intValue());
            trainingData.add(((PigVector) input.get(1)).getV());
        }
    }

    /**
     * Called when all tuples from current key have been passed to accumulate.
     *
     * @return the trained model.
     */
    public Tuple getValue() {
        Random gen = new Random();
        for (int i = 0; i < iterations * trainingData.size(); i++) {
            int k = gen.nextInt(trainingData.size());
            model.train(targetValues.get(k), trainingData.get(k));
        }

        try {
            Tuple r = new DefaultTuple();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            PolymorphicWritable.write(out, getModel());
            out.close();
            r.set(0, new DataByteArray(baos.toByteArray()));
            return r;
        } catch (IOException e) {
            // should never happen
            throw new ImpossibleStateError("Can't put results into tuple", e);
        }
    }

    /**
     * Called after getValue() to prepare processing for next key.
     */
    public void cleanup() {
        // nothing to clean up
    }

    public OnlineLogisticRegression getModel() {
        return model;
    }
}
