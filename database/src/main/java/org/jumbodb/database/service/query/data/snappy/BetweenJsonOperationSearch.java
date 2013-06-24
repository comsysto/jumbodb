package org.jumbodb.database.service.query.data.snappy;

import org.jumbodb.common.query.QueryClause;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class BetweenJsonOperationSearch implements JsonOperationSearch {
    @Override
    public boolean matches(QueryClause queryClause, Object value) {
        List<Number> searchValue = (List<Number>) queryClause.getValue();
        Number searchFrom = searchValue.get(0);
        Number searchTo = searchValue.get(1);
        if(value instanceof Double) {
            Double dv = (Double) value;
            return searchFrom.doubleValue() < dv && dv < searchTo.doubleValue();

        } else if(value instanceof Float) {
            Float dv = (Float) value;
            return searchFrom.floatValue() < dv && dv < searchTo.floatValue();

        } else if(value instanceof Integer) {
            Integer dv = (Integer) value;
            return searchFrom.intValue() < dv && dv < searchTo.intValue();

        } else if(value instanceof Long) {
            Long dv = (Long) value;
            return searchFrom.longValue() < dv && dv < searchTo.longValue();
        } else {
            throw new IllegalArgumentException(value.getClass().getSimpleName() + " is not supported for this search type.");
        }
    }
}
