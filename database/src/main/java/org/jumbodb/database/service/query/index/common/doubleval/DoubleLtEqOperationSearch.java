package org.jumbodb.database.service.query.index.common.doubleval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.numeric.NumberLtOperationSearch;

/**
 * @author Carsten Hufe
 */
public class DoubleLtEqOperationSearch extends NumberLtOperationSearch<Double, Double, NumberIndexFile<Double>> {

    @Override
    public boolean matching(Double currentValue, QueryValueRetriever queryValueRetriever) {
        Double searchValue = queryValueRetriever.getValue();
        return currentValue <= searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Double> snappyIndexFile) {
        Double searchValue = queryValueRetriever.getValue();
        return searchValue >= snappyIndexFile.getTo() || searchValue >= snappyIndexFile.getFrom();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new DoubleQueryValueRetriever(indexQuery);
    }
}
