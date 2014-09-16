package org.jumbodb.database.service.query.data.common;

/**
 * @author Carsten Hufe
 */
public class NeDataOperationSearch implements DataOperationSearch {
    @Override
    public boolean matches(Object leftValue, Object rightValue) {
        if (leftValue instanceof Number) {
            Number rightNumber = (Number) rightValue;
            if (leftValue instanceof Double) {
                Double leftDouble = (Double) leftValue;
                return leftDouble != rightNumber.doubleValue();
            } else if (leftValue instanceof Long) {
                Long leftLong = (Long) leftValue;
                return leftLong != rightNumber.longValue();
            } else if (leftValue instanceof Float) {
                Float leftFloat = (Float) leftValue;
                return leftFloat != rightNumber.floatValue();

            } else if (leftValue instanceof Integer) {
                Integer leftInt = (Integer) leftValue;
                return leftInt != rightNumber.intValue();
            }
        }
        if (leftValue != null) {
            return !leftValue.equals(rightValue);
        }
        return rightValue != null;
    }
}
