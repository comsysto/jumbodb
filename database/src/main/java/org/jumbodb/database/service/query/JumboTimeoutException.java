package org.jumbodb.database.service.query;

/**
 * @author Carsten Hufe
 */
public class JumboTimeoutException extends RuntimeException {
    public JumboTimeoutException(String s) {
        super(s);
    }
}
