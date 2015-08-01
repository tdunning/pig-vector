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

import org.antlr.runtime.RecognitionException;
import org.apache.mahout.pig.encoders.ArgumentEncoder;
import org.apache.mahout.pig.encoders.Schema;
import org.apache.mahout.pig.encoders.SchemaParseException;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;
import org.apache.mahout.vectorizer.encoders.LuceneTextValueEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;
import org.apache.mahout.vectorizer.encoders.TextValueEncoder;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

public class SchemaTest {
    @Test
    public void testBasics() throws RecognitionException, IOException {
        Map<String, ArgumentEncoder> r = Schema.parse("a:numeric, b:word, c:text(org.apache.lucene.analysis.en.EnglishAnalyzer), d:text");
        assertEquals(4, r.size());
        assertTrue(r.get("a").getEncoder() instanceof ContinuousValueEncoder);
        assertTrue(r.get("b").getEncoder() instanceof StaticWordValueEncoder);
        assertTrue(r.get("c").getEncoder() instanceof LuceneTextValueEncoder);
        assertTrue(r.get("d").getEncoder() instanceof TextValueEncoder);
    }

    @Test
    public void testFancyPositioning() throws RecognitionException, IOException {
        Map<String, ArgumentEncoder> r = Schema.parse("a:numeric, b$0:word, c$1:text(org.apache.lucene.analysis.en.EnglishAnalyzer), d:text");
        assertEquals(4, r.size());
        assertTrue(r.get("a").getEncoder() instanceof ContinuousValueEncoder);
        assertTrue(r.get("b").getEncoder() instanceof StaticWordValueEncoder);
        assertTrue(r.get("c").getEncoder() instanceof LuceneTextValueEncoder);
        assertTrue(r.get("d").getEncoder() instanceof TextValueEncoder);

        assertEquals(0, r.get("a").getPosition());
        assertEquals(0, r.get("b").getPosition());
        assertEquals(1, r.get("c").getPosition());
        assertEquals(1, r.get("d").getPosition());
    }

    @Test
    public void testMultipleDefinition() throws RecognitionException, IOException {
        try {
            Schema.parse("a:numeric, b:word, a:text, d:text");
            fail("Should have thrown syntax error");
        } catch (SchemaParseException e) {
            assertTrue(e.getMessage().contains("multiply defined"));
        }
    }

    @Test
    public void testSyntaxError() throws RecognitionException, IOException {
        try {
            Schema.parse("a, b:word, a:text, d:text");
            fail("Should have thrown syntax error");
        } catch (SchemaParseException e) {
            assertTrue(e.getMessage().contains("Syntax error"));
        }
    }
}
