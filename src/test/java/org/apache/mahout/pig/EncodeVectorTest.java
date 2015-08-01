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

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.pig.encoders.EncodeVector;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class EncodeVectorTest {

    @Test
    public void testSimple() throws IOException {
        EncodeVector ev = new EncodeVector("100", "1+x+y", "x:numeric, not:text, y:text");
        Tuple t = TupleFactory.getInstance().newTuple(4);
        t.set(0, 3.1);
        t.set(1, "foo bar");
        t.set(2, "");
        t.set(3, null);

        Vector v1 = PigVector.fromBytes(ev.exec(t));
        assertEquals(3.1, v1.get(10), 0);
        assertEquals(1.0, v1.get(39), 0);

        t.set(2, "fob");

        Vector pv = PigVector.fromBytes(ev.exec(t));
        Vector v2 = pv.assign(v1, Functions.MINUS);
        assertEquals(1, v2.get(40), 0);
        assertEquals(1, v2.get(55), 0);

        t.set(2, "lots of text for testing");

        pv = PigVector.fromBytes(ev.exec(t));
        Vector v3 = pv.assign(v1, Functions.MINUS);
        assertEquals(10, v3.zSum(), 0);
        assertEquals(1, v3.maxValue(), 1);

        EncodeVector ev2 = new EncodeVector("100", "x+y", "x:numeric, not:text, y:text");
        t.set(2, "");
        pv = PigVector.fromBytes(ev2.exec(t));
        Vector v4 = pv;
        assertEquals(3.1, v1.get(10), 0);
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
