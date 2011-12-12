package org.apache.mahout.pig;

import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

/**
 * A schema specifies the names and types for positional parameters in a tuple.  These names
 * and types are used when encoding tuples as vectors using PigVector.
 *
 * This class contains convenience routines that simplify the use of the schema parser classes.
 *
 * A schema consists of a comma separated list of variable specifications.  A variable
 * specification contains a name plus optional absolute position and a type separated by a colon.
 * The name follows the rules of java identifiers.  The optional absolute position is indicated
 * by a dollar sign and a number.  The number indicates the 0-based position of the variable
 * in any tuple being encoded.  Variables without positions are assigned consecutive positions
 * starting at 0 in the tuple.
 *
 * The type specification can be numeric, word or text.  For text, you can supply an optional
 * parenthesis surrounded fully qualified name of a Java class that implements the Analyzer
 * interface from Lucene.  If you don't supply an analyzer, the text is tokenized on whilte-
 * space boundaries.
 *
 * Here is an example of a schema:
 * <pre>
 * a:numeric, b$0:word, c$1:text(org.apache.lucene.analysis.en.EnglishAnalyzer), d:text
 * </pre>
 * Here, argument 0 in the encoded tuple is encoded as a number and as a key word while
 * argument 1 in the encoded tuple is encoded as text using two different methods for
 * analysis.
 */
public class Schema {
    public static Map<String, ArgumentEncoder> parse(Reader reader) {
        SchemaParser tokenParser = null;
        try {
            CommonTokenStream input = new CommonTokenStream(new SchemaLexer(new ANTLRReaderStream(reader)));
            tokenParser = new SchemaParser(input);
            SchemaParser.schema_return parserResult = tokenParser.schema();
        } catch (IOException e) {
            throw new SchemaParseException("Cannot parse schema", e);
        } catch (RecognitionException e) {
            throw new SchemaParseException("Cannot parse schema", e);
        }
        return tokenParser.getEncoders();
    }

    static Map<String, ArgumentEncoder> parse(String s) {
        final StringReader input = new StringReader(s);
        Map<String, ArgumentEncoder> r = parse(input);
        input.close();
        return r;
    }
}
