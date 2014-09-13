package org.jumbodb.database.service.query.index.floatval.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

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
