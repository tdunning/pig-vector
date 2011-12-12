package org.apache.mahout.pig;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EncodeVectorTest {

    @Test
    public void testSimple() throws IOException {
        EncodeVector ev = new EncodeVector("100", "1+x+y", "x:numeric, not:text, y:text");
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, 3.1);
        t.set(1, "foo bar");
        t.set(2, "");
        t.set(3, null);

        Tuple v1 = ev.exec(t);
        assertTrue(v1 instanceof PigVector);
        assertEquals(3.1, (Double) v1.get(10), 0);
        assertEquals(1.0, (Double) v1.get(39), 0);

        t.set(2, "fob");

        Vector v2 = ((PigVector) ev.exec(t)).getV().assign(((PigVector) v1).getV(), Functions.MINUS);
        assertEquals(1, v2.get(40), 0);
        assertEquals(1, v2.get(55), 0);

        t.set(2, "lots of text for testing");

        Vector v3 = ((PigVector) ev.exec(t)).getV().assign(((PigVector) v1).getV(), Functions.MINUS);
        assertEquals(10, v3.zSum(), 0);
        assertEquals(1, v3.maxValue(), 1);

        EncodeVector ev2 = new EncodeVector("100", "x+y", "x:numeric, not:text, y:text");
        t.set(2, "");
        Vector v4 = ((PigVector) ev2.exec(t)).getV();
        assertEquals(3.1, (Double) v1.get(10), 0);
        assertEquals(3.1, v4.zSum(), 0);
        assertEquals(3.1, v4.maxValue(), 1);
    }

    @Test
    public void testBadIndex() throws IOException {
        EncodeVector ev2 = new EncodeVector("100", "x+y", "x:numeric, not:text, y:text");
        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, 3.1);
        t.set(1, "foo bar");
        try {
            ev2.exec(t);
            fail("Should have failed because tuple is too small");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Tuple doesn't have at least"));
        }
    }
}
