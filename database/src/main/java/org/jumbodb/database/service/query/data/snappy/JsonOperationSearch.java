package org.jumbodb.database.service.query.data.snappy;

import org.jumbodb.common.query.JsonQuery;
import org.jumbodb.common.query.QueryClause;

/**
 * @author Carsten Hufe
 */
public interface JsonOperationSearch {
    boolean matches(JsonQuery jsonQuery, Object value);
}
