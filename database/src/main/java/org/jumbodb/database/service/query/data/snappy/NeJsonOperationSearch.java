package org.jumbodb.database.service.query.data.snappy;

import org.jumbodb.common.query.JsonQuery;

/**
 * @author Carsten Hufe
 */
public class NeJsonOperationSearch implements JsonOperationSearch {
    @Override
    public boolean matches(JsonQuery jsonQuery, Object value) {
        return !jsonQuery.getValue().equals(value);
    }
}
