package org.jumbodb.database.service.query.index.common.longval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class LongQueryValueRetriever implements QueryValueRetriever {
    private Long value;

    public LongQueryValueRetriever(IndexQuery indexQuery) {
        value = ((Number) indexQuery.getValue()).longValue();
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
