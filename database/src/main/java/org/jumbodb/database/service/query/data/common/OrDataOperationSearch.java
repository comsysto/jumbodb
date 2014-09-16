package org.jumbodb.database.service.query.data.common;

/**
 * @author Carsten Hufe
 */
// CARSTEN unit test
public class OrDataOperationSearch implements DataOperationSearch {
    @Override
    public boolean matches(Object leftValue, Object rightValue) {
        return true;
    }
}
