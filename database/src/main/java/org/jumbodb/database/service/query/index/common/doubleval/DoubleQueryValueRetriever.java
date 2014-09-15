package org.jumbodb.database.service.query.index.common.doubleval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class DoubleQueryValueRetriever implements QueryValueRetriever {
    private Double value;

    public DoubleQueryValueRetriever(IndexQuery indexQuery) {
        value = ((Number) indexQuery.getValue()).doubleValue();
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
