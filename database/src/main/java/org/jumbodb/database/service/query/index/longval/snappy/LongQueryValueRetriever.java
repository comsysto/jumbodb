package org.jumbodb.database.service.query.index.longval.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

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
