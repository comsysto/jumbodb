package org.jumbodb.database.service.importer;

/**
 * Thrown when the delivery already exists
 */
public class DeliveryVersionExistsException extends Exception {
    public DeliveryVersionExistsException(final String s) {
        super(s);
    }
}
