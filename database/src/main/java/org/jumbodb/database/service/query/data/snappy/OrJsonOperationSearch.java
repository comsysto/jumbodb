package org.jumbodb.database.service.query.data.snappy;

import org.jumbodb.common.query.JsonQuery;

/**
 * @author Carsten Hufe
 */
// CARSTEN unit test
public class OrJsonOperationSearch implements JsonOperationSearch {
    @Override
    public boolean matches(JsonQuery jsonQuery, Object value) {
        return true;
    }
}
