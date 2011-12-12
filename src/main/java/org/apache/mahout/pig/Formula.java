package org.apache.mahout.pig;

import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: tdunning
 * Date: 12/12/11
 * Time: 10:01 AM
 * To change this template use File | Settings | File Templates.
 */
public class Formula {
        public static EncodingSpec parse(Reader reader) {
        FormulaParser tokenParser = null;
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
