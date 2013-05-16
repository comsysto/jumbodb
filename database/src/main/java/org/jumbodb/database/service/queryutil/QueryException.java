package org.jumbodb.database.service.queryutil;

/**
 * @author Carsten Hufe
 */
public class QueryException extends Exception {
    public QueryException(String s) {
        super(s);
    }

    public QueryException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
