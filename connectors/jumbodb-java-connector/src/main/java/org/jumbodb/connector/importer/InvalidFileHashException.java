package org.jumbodb.connector.importer;

/**
 * @author Carsten Hufe
 */
public class InvalidFileHashException extends Exception {
    public InvalidFileHashException(String message) {
        super(message);
    }
}
