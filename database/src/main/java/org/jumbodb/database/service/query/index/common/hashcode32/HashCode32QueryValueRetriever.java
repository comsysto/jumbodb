package org.jumbodb.database.service.query.index.common.hashcode32;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class HashCode32QueryValueRetriever implements QueryValueRetriever {
    private Integer value;

    public HashCode32QueryValueRetriever(IndexQuery indexQuery) {
        value = indexQuery.getValue().hashCode();
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
