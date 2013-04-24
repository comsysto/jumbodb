package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class IntegerQueryValueRetriever implements QueryValueRetriever {
    private Integer value;

    public IntegerQueryValueRetriever(QueryClause queryClause) {
        value = ((Number) queryClause.getValue()).intValue();
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
