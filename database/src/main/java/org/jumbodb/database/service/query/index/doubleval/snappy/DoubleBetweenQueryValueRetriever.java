package org.jumbodb.database.service.query.index.doubleval.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class DoubleBetweenQueryValueRetriever implements QueryValueRetriever {
    private List<Double> value;

    public DoubleBetweenQueryValueRetriever(QueryClause queryClause) {
        value = new ArrayList<Double>(2);
        List<? extends Number> vals = (List<? extends Number>) queryClause.getValue();
        for (Number val : vals) {
            value.add(val.doubleValue());
        }
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
