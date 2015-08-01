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

import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.mahout.pig.encoders.EncodingSpec;
import org.apache.mahout.pig.encoders.FormulaLexer;
import org.apache.mahout.pig.encoders.FormulaParseException;
import org.apache.mahout.pig.encoders.FormulaParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EncodingSpecTest {
    @Test
    public void testSum() throws RecognitionException, IOException {
        EncodingSpec z = parse("a+c+b");
        Assert.assertEquals("[[a], [b], [c]]", z.getVariables().toString());

        try {
            parse("a+$1+$2");
            fail("Should have failed");
        } catch (FormulaParseException e) {
            assertTrue(e.getMessage().startsWith("Syntax error"));
        }

        z = parse("a + 1");
        Assert.assertEquals("[[1], [a]]", z.getVariables().toString());
    }

    @Test
    public void testWrongBias() throws RecognitionException, IOException {
        try {
            parse("a + 2");
            fail("Should have failed due to invalid bias");
        } catch (FormulaParseException e) {
            assertTrue(e.getMessage().startsWith("Invalid bias"));
        }
    }

    @Test
    public void testProduct() throws RecognitionException, IOException {
        EncodingSpec z = parse("a+c*b");
        Assert.assertEquals("[[a], [b], [b, c], [c]]", z.getVariables().toString());
        z = parse("(a+c)*(c+b+a)");
        Assert.assertEquals("[[a], [a, b], [a, c], [b], [b, c], [c]]", z.getVariables().toString());
    }

    @Test
    public void testPower() throws RecognitionException, IOException {
        EncodingSpec z = parse("(a+b+c)^2");
        Assert.assertEquals("[[a], [a, b], [a, c], [b], [b, c], [c]]", z.getVariables().toString());

        z = parse("(a+b+c)^3");
        Assert.assertEquals("[[a], [a, b], [a, b, c], [a, c], [b], [b, c], [c]]", z.getVariables().toString());

        z = parse("(a+b+c)^30");
        Assert.assertEquals("[[a], [a, b], [a, b, c], [a, c], [b], [b, c], [c]]", z.getVariables().toString());
    }

    private EncodingSpec parse(String s) throws IOException, RecognitionException {
        Reader reader = new StringReader(s);
        CommonTokenStream input = new CommonTokenStream(new FormulaLexer(new ANTLRReaderStream(reader)));
        FormulaParser tokenParser = new FormulaParser(input);
        FormulaParser.expression_return parserResult = tokenParser.expression();
        reader.close();
        return parserResult.r;
    }

}
