package org.jumbodb.database.service.query.index.hashcode64.snappy;

import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.HashCode64;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Carsten Hufe
 */
public class HashCode64QueryValueRetriever implements QueryValueRetriever {
    private Long value;

    public HashCode64QueryValueRetriever(QueryClause queryClause) {
        Object objValue = queryClause.getValue();
        if(objValue instanceof String) {
            value = HashCode64.hash((String) objValue);
        }
        else if(objValue instanceof Long) {
            value = (Long) objValue;
        }
        else if(objValue instanceof Double) {
            value = Double.doubleToLongBits((Double)objValue);
        }
        else {
            throw new IllegalArgumentException("Value type " + objValue.getClass() + " for HashCode64 is not supported");
        }
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
