package org.jumbodb.database.service.query.data.common;

import org.jumbodb.common.query.JsonQuery;

/**
 * @author Carsten Hufe
 */
public class GtDataOperationSearch implements DataOperationSearch {
    @Override
    public boolean matches(JsonQuery jsonQuery, Object value) {
        Number searchValue = (Number) jsonQuery.getValue();
        if(value instanceof Double) {
            Double dv = (Double) value;
            return dv > searchValue.doubleValue();

        } else if(value instanceof Float) {
            Float dv = (Float) value;
            return dv > searchValue.floatValue();

        } else if(value instanceof Integer) {
            Integer dv = (Integer) value;
            return dv > searchValue.intValue();

        } else if(value instanceof Long) {
            Long dv = (Long) value;
            return dv > searchValue.longValue();
        } else {
            throw new IllegalArgumentException(value.getClass().getSimpleName() + " is not supported for this search type.");
        }
    }
}