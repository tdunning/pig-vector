package org.apache.mahout.pig.encoders;

public class FormulaParseException extends RuntimeException {
    public FormulaParseException(Throwable e, String msg) {
        super(msg, e);
    }
}
