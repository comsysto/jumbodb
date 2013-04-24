package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class IntegerQueryValueRetriever implements QueryValueRetriever {
    private Integer value;

    public IntegerQueryValueRetriever(QueryClause queryClause) {
        value = (Integer) queryClause.getValue();
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
