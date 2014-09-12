package org.jumbodb.database.service.importer;

/**
 * Thrown when the file has a different checksum
 */
public class FileChecksumException extends Exception {
    public FileChecksumException(final String s) {
        super(s);
    }
}
