package org.jumbodb.database.service.query.index.longval.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class LongBetweenQueryValueRetriever implements QueryValueRetriever {
    private List<Long> value;

    public LongBetweenQueryValueRetriever(QueryClause queryClause) {
        value = (List<Long>) queryClause.getValue();
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
