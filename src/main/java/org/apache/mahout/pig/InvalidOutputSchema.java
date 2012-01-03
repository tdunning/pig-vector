package org.apache.mahout.pig;

import java.io.IOException;

public class InvalidOutputSchema extends IOException {
    public InvalidOutputSchema(String msg) {
        super(msg);
    }
}
