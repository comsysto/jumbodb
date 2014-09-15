package org.jumbodb.database.service.query.index.common.floatval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class FloatBetweenQueryValueRetriever implements QueryValueRetriever {
    private List<Float> value;

    public FloatBetweenQueryValueRetriever(IndexQuery indexQuery) {
        value = new ArrayList<Float>(2);
        List<? extends Number> vals = (List<? extends Number>) indexQuery.getValue();
        for (Number val : vals) {
            value.add(val.floatValue());
        }
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}