package org.jumbodb.database.service.query.data.snappy;

import org.jumbodb.common.query.QueryClause;

/**
 * @author Carsten Hufe
 */
public interface JsonOperationSearch {
    boolean matches(QueryClause queryClause, Object value);
}
