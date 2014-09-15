package org.jumbodb.database.service.query.index.common.longval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

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