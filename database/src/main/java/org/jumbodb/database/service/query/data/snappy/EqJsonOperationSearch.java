package org.jumbodb.database.service.query.data.snappy;

import org.jumbodb.common.query.QueryClause;

/**
 * @author Carsten Hufe
 */
public class EqJsonOperationSearch implements JsonOperationSearch {
    @Override
    public boolean matches(QueryClause queryClause, Object value) {
        return queryClause.getValue().equals(value);
    }
}
