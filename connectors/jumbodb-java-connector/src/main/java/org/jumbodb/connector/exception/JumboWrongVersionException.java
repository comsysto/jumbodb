package org.jumbodb.connector.exception;

/**
 * @author Carsten Hufe
 */
public class JumboWrongVersionException extends RuntimeException {
    public JumboWrongVersionException(String s) {
        super(s);
    }
}
