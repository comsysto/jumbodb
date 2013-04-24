package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class IntegerBetweenQueryValueRetriever implements QueryValueRetriever {
    private List<Integer> value;

    public IntegerBetweenQueryValueRetriever(QueryClause queryClause) {
        value = (List<Integer>) queryClause.getValue();
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
