package org.jumbodb.database.service.query.index.hashcode.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class HashCodeQueryValueRetriever implements QueryValueRetriever {
    private Integer value;

    public HashCodeQueryValueRetriever(QueryClause queryClause) {
        value = queryClause.getValue().hashCode();
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
