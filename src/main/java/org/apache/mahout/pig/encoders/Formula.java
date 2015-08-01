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

package org.apache.mahout.pig.encoders;

import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

public class Formula {
    public static EncodingSpec parse(Reader reader) {
        FormulaParser tokenParser;
        try {
            CommonTokenStream input = new CommonTokenStream(new FormulaLexer(new ANTLRReaderStream(reader)));
            tokenParser = new FormulaParser(input);
            FormulaParser.expression_return parserResult = tokenParser.expression();
            return parserResult.r;
        } catch (IOException e) {
            throw new SchemaParseException("Cannot parse schema", e);
        } catch (RecognitionException e) {
            throw new SchemaParseException("Cannot parse schema", e);
        }
    }

    public static EncodingSpec parse(String formula) {
        final StringReader input = new StringReader(formula);
        EncodingSpec r = parse(input);
        input.close();
        return r;
    }
}
