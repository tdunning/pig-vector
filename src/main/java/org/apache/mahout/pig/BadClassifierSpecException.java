package org.apache.mahout.pig;

/**
 * Signal an error in classifier construction.
 */
public class BadClassifierSpecException extends RuntimeException {
    public BadClassifierSpecException(String msg, Throwable e) {
        super(msg, e);
    }

    public BadClassifierSpecException(String msg) {
        super(msg);
    }
}
