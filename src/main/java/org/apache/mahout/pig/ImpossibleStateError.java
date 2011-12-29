package org.apache.mahout.pig;

import java.io.IOException;

/**
 * Indicates an "impossible" condition has been detected that prevents further progress.
 */
public class ImpossibleStateError extends RuntimeException {
    public ImpossibleStateError(String msg) {
        super(msg);
    }

    public ImpossibleStateError(String msg, IOException e) {
        super(msg, e);
    }
}
