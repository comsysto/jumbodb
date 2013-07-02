package org.jumbodb.database.service.query;

/**
 * @author Carsten Hufe
 */
public class JumboIndexMissingException extends RuntimeException {
    public JumboIndexMissingException(String s) {
        super(s);
    }
}
