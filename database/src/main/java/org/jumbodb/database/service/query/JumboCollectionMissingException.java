package org.jumbodb.database.service.query;

/**
 * @author Carsten Hufe
 */
public class JumboCollectionMissingException extends RuntimeException {
    public JumboCollectionMissingException(String s) {
        super(s);
    }
}
