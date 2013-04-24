package org.jumbodb.database.service.query.index.doubleval.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class DoubleQueryValueRetriever implements QueryValueRetriever {
    private Double value;

    public DoubleQueryValueRetriever(QueryClause queryClause) {
        value = ((Number) queryClause.getValue()).doubleValue();
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
