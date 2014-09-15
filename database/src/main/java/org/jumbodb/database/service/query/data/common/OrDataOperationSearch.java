package org.jumbodb.database.service.query.data.common;

import org.jumbodb.common.query.JsonQuery;

/**
 * @author Carsten Hufe
 */
// CARSTEN unit test
public class OrDataOperationSearch implements DataOperationSearch {
    @Override
    public boolean matches(JsonQuery jsonQuery, Object value) {
        return true;
    }
}
