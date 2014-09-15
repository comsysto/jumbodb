package org.jumbodb.database.service.query.data.common;

import org.jumbodb.common.query.JsonQuery;

/**
 * @author Carsten Hufe
 */
public class NeDataOperationSearch implements DataOperationSearch {
    @Override
    public boolean matches(JsonQuery jsonQuery, Object value) {
        return !jsonQuery.getValue().equals(value);
    }
}
