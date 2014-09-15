package org.jumbodb.database.service.query.data.common;

import org.jumbodb.common.query.JsonQuery;

/**
 * @author Carsten Hufe
 */
public interface DataOperationSearch {
    boolean matches(JsonQuery jsonQuery, Object value);
}
