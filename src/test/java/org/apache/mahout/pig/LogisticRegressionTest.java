package org.apache.mahout.pig;

import com.google.common.collect.ImmutableList;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.classifier.sgd.PolymorphicWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.DoubleFunction;
import org.apache.pig.data.*;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertTrue;

public class LogisticRegressionTest {
    @Test
    public void testConstructor() throws IOException {
        LogisticRegression lr = new LogisticRegression("categories = a b c, features=10000");
        assertEquals(3, lr.getModel().numCategories());
        assertEquals(10000, lr.getModel().numFeatures());
        assertEquals(1, lr.getIterations());
        assertTrue(lr.isInMemory());


        lr = new LogisticRegression("categories = a b c, features=10000, decayExponent=0.3 ,stepOffset=123, learningRate=2.1, lambda=0.001, iterations=12, inMemory=false");
        assertEquals(2.1 * Math.pow(123, -0.3), lr.getModel().currentLearningRate());
        assertEquals(0.001, lr.getModel().getLambda());

        try {
            new LogisticRegression("categories = a , features=10000");
            fail("Should have failed");
        } catch (BadClassifierSpecException e) {
            assertTrue("Single target category", e.getMessage().startsWith("Must have more than one target category"));
        }

        try {
            new LogisticRegression("categories = a b, features=10000, x=3");
            fail("Should have failed");
        } catch (BadClassifierSpecException e) {
            assertEquals("Extra options", "Extra options supplied: x", e.getMessage());
        }

        try {
            new LogisticRegression("categories = a ");
            fail("Should have failed");
        } catch (BadClassifierSpecException e) {
            assertEquals("No features", "Must specify previous model location using \"file\" or supply \"categories\" and \"features\"", e.getMessage());
        }
    }

    @Test
    public void testTraining() throws IOException {
        DoubleFunction randomValue = new DoubleFunction() {
            private Random gen = new Random(1);

            public double apply(double arg1) {
                return gen.nextGaussian();
            }
        };

        // start with a random direction
        Vector n = new DenseVector(4);
        n.assign(randomValue);

        // make up data.  result is 1 if in the same rough direction as n, else 0
        String[] target = new String[]{"0", "1"};
        DataBag examples = new DefaultDataBag();
        for (int i = 0; i < 400; i++) {
            Vector v = new DenseVector(4);
            v.assign(randomValue);

            Tuple x = new DefaultTuple();
            x.append(target[v.dot(n) > 0 ? 1 : 0]);
            x.append(new PigVector(v));
            examples.add(x);
        }
        Tuple data = new DefaultTuple();
        data.append(examples);

        // train model.  training from tmp file allows absolute repeatability
        LogisticRegression lr = new LogisticRegression("categories = 0 1, features=4, inMemory=false, iterations=5");
        lr.accumulate(data);
        Tuple r = lr.getValue();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(((DataByteArray) r.get(0)).get()));
        OnlineLogisticRegression model = PolymorphicWritable.read(in, OnlineLogisticRegression.class);
        assertEquals(lr.getModel().currentLearningRate(), model.currentLearningRate(), 1e-10);
        in.close();

        // with that many data points, model should point in the same direction as the original vector
        Vector v = model.getBeta().viewRow(0);
        double z = n.dot(v) / (n.norm(2) * v.norm(2));
        assertEquals(1.0, z, 1e-2);

        // just for grins, we should check whether the model actually computes the correct values
        List<String> categories = ImmutableList.of("0", "1");
        for (Tuple example : examples) {
            double score = model.classifyScalar(((PigVector) example.get(1)).getV());
            int actual = categories.indexOf(example.get(0));
            score = score * actual + (1 - score) * (1 - actual);
            assertTrue(score > 0.4);
        }
    }
}
