package org.jumbodb.database.service.query.data.snappy;

import org.jumbodb.common.query.QueryClause;

/**
 * @author Carsten Hufe
 */
public class LtJsonOperationSearch implements JsonOperationSearch {
    @Override
    public boolean matches(QueryClause queryClause, Object value) {
        Number searchValue = (Number) queryClause.getValue();
        if(value instanceof Double) {
            Double dv = (Double) value;
            return dv < searchValue.doubleValue();

        } else if(value instanceof Float) {
            Float dv = (Float) value;
            return dv < searchValue.floatValue();

        } else if(value instanceof Integer) {
            Integer dv = (Integer) value;
            return dv < searchValue.intValue();

        } else if(value instanceof Long) {
            Long dv = (Long) value;
            return dv < searchValue.longValue();
        } else {
            throw new UnsupportedOperationException(value.getClass().getSimpleName() + " is not supported for this search type.");
        }
    }
}
