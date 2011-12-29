package org.apache.mahout.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.classifier.sgd.PolymorphicWritable;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Filter UDF that uses a pre-existing classifier to classify a bunch of data.
 */
public class Classifier extends EvalFunc<Tuple> {
    private final OnlineLogisticRegression model;

    public Classifier(String modelClassName, String file) {
        try {
            Class<OnlineLogisticRegression> modelClass = (Class<OnlineLogisticRegression>) Class.forName(modelClassName);
            final Configuration conf = UDFContext.getUDFContext().getJobConf();
            model = PolymorphicWritable.read(FileSystem.get(conf).open(new Path(file)), modelClass);
        } catch (ClassNotFoundException e) {
            throw new BadClassifierSpecException("Can't find model class: " + modelClassName, e);
        } catch (FileNotFoundException e) {
            throw new BadClassifierSpecException("Can't find file to read model from: " + file, e);
        } catch (IOException e) {
            throw new BadClassifierSpecException("Error while reading model from: " + file, e);
        }
    }

    /**
     * Tuples contain test data.  If the tuple has a single element, we assume it is a PigVector that we should
     * classify and we should return a tuple containing the most likely category and the probabilities for each
     * category (as a PigVector).  If the input holds two elements, then we should return most likely category,
     * a PigVector containing the probabilities, log-likelihood and correctness of the classifier encoded as 1 for
     * correct and 0 for incorrect.
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
