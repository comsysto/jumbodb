package org.jumbodb.database.service.query.data.common;

import org.jumbodb.common.query.JsonQuery;

/**
 * @author Carsten Hufe
 */
public interface DataOperationSearch {
    // CARSTEN change signature to     boolean matches(Object leftValue, Object rightValue);
    boolean matches(JsonQuery jsonQuery, Object value);
}
