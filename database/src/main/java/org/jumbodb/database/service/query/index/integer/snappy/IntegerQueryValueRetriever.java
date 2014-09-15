package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class IntegerQueryValueRetriever implements QueryValueRetriever {
    private Integer value;

    public IntegerQueryValueRetriever(IndexQuery indexQuery) {
        value = ((Number) indexQuery.getValue()).intValue();
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
