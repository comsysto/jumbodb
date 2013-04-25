package org.jumbodb.database.service.query.index.doubleval.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberLtOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class DoubleLtOperationSearch extends NumberLtOperationSearch<Double, Double, NumberSnappyIndexFile<Double>> {
    public DoubleLtOperationSearch(NumberSnappyIndexStrategy<Double, Double, NumberSnappyIndexFile<Double>> strategy) {
        super(strategy);
    }

    @Override
    public boolean matching(Double currentValue, QueryValueRetriever queryValueRetriever) {
        Double searchValue = queryValueRetriever.getValue();
        return currentValue < searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Double> snappyIndexFile) {
        Double searchValue = queryValueRetriever.getValue();
        return searchValue < snappyIndexFile.getTo();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new DoubleQueryValueRetriever(queryClause);
    }
}
