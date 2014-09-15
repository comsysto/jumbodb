package org.jumbodb.database.service.query.index.doubleval.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.basic.numeric.NumberEqOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class DoubleEqOperationSearch extends NumberEqOperationSearch<Double, Double, Double, NumberSnappyIndexFile<Double>> {

    @Override
    public boolean matching(Double currentValue, QueryValueRetriever queryValueRetriever) {
        Double searchValue = queryValueRetriever.getValue();
        return currentValue.equals(searchValue);
    }

    @Override
    public boolean eq(Double val1, Double val2) {
        return val1.equals(val2);
    }

    @Override
    public boolean lt(Double val1, Double val2) {
        return val1 < val2;
    }

    @Override
    public boolean gt(Double val1, Double val2) {
        return val1 > val2;
    }

    @Override
    public boolean ltEq(Double val1, Double val2) {
        return val1 <= val2;
    }

    @Override
    public boolean gtEq(Double val1, Double val2) {
        return val1 >= val2;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberSnappyIndexFile<Double> snappyIndexFile) {
        Double searchValue = queryValueRetriever.getValue();
        return searchValue >= snappyIndexFile.getFrom() && searchValue <= snappyIndexFile.getTo();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new DoubleQueryValueRetriever(indexQuery);
    }
}
