package org.jumbodb.database.service.query.index.common.floatval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class FloatQueryValueRetriever implements QueryValueRetriever {
    private Float value;

    public FloatQueryValueRetriever(IndexQuery indexQuery) {
        value = ((Number) indexQuery.getValue()).floatValue();
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
