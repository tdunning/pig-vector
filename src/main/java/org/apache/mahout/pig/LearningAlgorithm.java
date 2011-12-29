package org.apache.mahout.pig;

import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.classifier.sgd.PolymorphicWritable;
import org.apache.mahout.pig.annotations.Stable;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

@Stable
public class LearningAlgorithm extends EvalFunc<Tuple> implements Accumulator<Tuple> {
    OnlineLogisticRegression model;

    public LearningAlgorithm(String modelClass, String modelParams) {
        // TODO parse learning parameters
        // lambda, decayExponent, offset, file holding previous model

    }

    /**
     * This callback method must be implemented by all subclasses. This
     * is the method that will be invoked on every Tuple of a given dataset.
     * Since the dataset may be divided up in a variety of ways the programmer
     * should not make assumptions about state that is maintained between
     * invocations of this method.
     *
     * @param input the Tuple to be processed.
     * @return result, of type T.
     * @throws java.io.IOException
     */
    @Override
    public Tuple exec(Tuple input) throws IOException {
        getModel().train(((Number) input.get(0)).intValue(), ((PigVector) input.get(1)).getV());
        return getValue();
    }

    /**
     * Pass tuples to the UDF.
     *
     * @param b A tuple containing a single field, which is a bag.  The bag will contain the set
     *          of tuples being passed to the UDF in this iteration.
     */
    public void accumulate(Tuple b) throws IOException {
        DataBag data = (DataBag) b.get(0);
        for (Tuple input : data) {
            getModel().train(((Number) input.get(0)).intValue(), ((PigVector) input.get(1)).getV());
        }
    }

    /**
     * Called when all tuples from current key have been passed to accumulate.
     *
     * @return the value for the UDF for this key.
     */
    public Tuple getValue() {
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
