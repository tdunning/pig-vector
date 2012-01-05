package org.apache.mahout.pig.encoders;

import org.apache.mahout.math.Vector;
import org.apache.mahout.pig.PigVector;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.*;

public class DirectVectorTest {
    @Test
    public void testEncoding() throws IOException {
        DirectVector dv = new DirectVector("a,b,c");
        final Tuple input = TupleFactory.getInstance().newTuple();
        input.append(1.0);
        input.append(2);
        input.append(3L);
        Vector v = PigVector.fromBytes(dv.exec(input));
        assertEquals(3, v.size());
        assertEquals(1.0, v.get(0), 0);
        assertEquals(2.0, v.get(1), 0);
        assertEquals(3.0, v.get(2), 0);
    }

    @Test
    public void testWrongSize() throws IOException {
        DirectVector dv = new DirectVector("a,b,c");
        final Tuple input = TupleFactory.getInstance().newTuple();
        input.append(1.0);
        input.append(2);
        try {
            dv.exec(input);
            fail("Should have noticed short tuple");
        } catch (IllegalArgumentException e) {
            assertEquals("Tuple doesn't have 3 elements", e.getMessage());
        }
    }


    @Test
    public void testNullAsNaN() throws IOException {
        DirectVector dv = new DirectVector("a,b,c");
        final Tuple input = TupleFactory.getInstance().newTuple();
        input.append(1.0);
        input.append(2);
        input.append(null);

        Vector v = PigVector.fromBytes(dv.exec(input));
        assertEquals(3, v.size());
        assertEquals(1.0, v.get(0), 0);
        assertEquals(2.0, v.get(1), 0);
        assertTrue(Double.isNaN(v.get(2)));
    }
}
