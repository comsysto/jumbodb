package org.jumbodb.database.service.query.index.longval.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class LongBetweenQueryValueRetriever implements QueryValueRetriever {
    private List<Long> value;

    public LongBetweenQueryValueRetriever(IndexQuery indexQuery) {
        value = new ArrayList<Long>(2);
        List<? extends Number> vals = (List<? extends Number>) indexQuery.getValue();
        for (Number val : vals) {
            value.add(val.longValue());
        }
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
