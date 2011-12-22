package org.apache.mahout.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class Classifier extends EvalFunc<Tuple> {
    public Classifier(String modelClass) {
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
        throw new UnsupportedOperationException("Default operation");
    }
}
