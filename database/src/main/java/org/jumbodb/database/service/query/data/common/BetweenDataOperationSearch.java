package org.jumbodb.database.service.query.data.common;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class BetweenDataOperationSearch implements DataOperationSearch {
    @Override
    public boolean matches(Object leftValue, Object rightValue) {
        if (isNotNumber(leftValue)) {
            return false;
        }
        List<Number> searchValue = (List<Number>) rightValue;
        Number searchFrom = searchValue.get(0);
        Number searchTo = searchValue.get(1);
        if (leftValue instanceof Double) {
            Double leftDouble = (Double) leftValue;
            return searchFrom.doubleValue() <= leftDouble && leftDouble <= searchTo.doubleValue();
        } else if (leftValue instanceof Long) {
            Long leftLong = (Long) leftValue;
            return searchFrom.longValue() <= leftLong && leftLong <= searchTo.longValue();
        } else if (leftValue instanceof Float) {
            Float leftFloat = (Float) leftValue;
            return searchFrom.floatValue() <= leftFloat && leftFloat <= searchTo.floatValue();
        } else if (leftValue instanceof Integer) {
            Integer leftInt = (Integer) leftValue;
            return searchFrom.intValue() <= leftInt && leftInt <= searchTo.intValue();
        } else {
            return false;
        }
    }

    private boolean isNotNumber(final Object value) {
        return !(value instanceof Number);
    }
}
