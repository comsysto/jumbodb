package org.jumbodb.database.service.query.data.common;

/**
 * @author Carsten Hufe
 */
public interface DataOperationSearch {
    boolean matches(Object leftValue, Object rightValue);
}
