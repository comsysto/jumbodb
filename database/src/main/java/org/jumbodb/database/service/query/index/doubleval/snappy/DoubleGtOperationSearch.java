package org.jumbodb.database.service.query.index.doubleval.snappy;

import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class DoubleGtOperationSearch extends DoubleEqOperationSearch {
    public DoubleGtOperationSearch(NumberSnappyIndexStrategy<Double, Double, NumberSnappyIndexFile<Double>> strategy) {
        super(strategy);
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Double> snappyIndexFile) {
        Double searchValue = queryValueRetriever.getValue();
        return searchValue > snappyIndexFile.getFrom();
    }

    @Override
    public boolean matching(Double currentValue, QueryValueRetriever queryValueRetriever) {
        Double searchValue = queryValueRetriever.getValue();
        return currentValue > searchValue;
    }
}
