package org.apache.mahout.pig;

import org.antlr.runtime.RecognitionException;
import org.apache.mahout.vectorizer.encoders.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
            Map<String, ArgumentEncoder> r = Schema.parse("a:numeric, b:word, a:text, d:text");
            fail("Should have thrown syntax error");
        } catch (SchemaParseException e) {
            assertTrue(e.getMessage().contains("multiply defined"));
        }
    }

    @Test
    public void testSyntaxError() throws RecognitionException, IOException {
        try {
            Map<String, ArgumentEncoder> r = Schema.parse("a, b:word, a:text, d:text");
            fail("Should have thrown syntax error");
        } catch (SchemaParseException e) {
            assertTrue(e.getMessage().contains("Syntax error"));
        }
    }
}
