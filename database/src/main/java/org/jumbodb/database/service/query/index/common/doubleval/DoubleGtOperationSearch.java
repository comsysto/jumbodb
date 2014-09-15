package org.jumbodb.database.service.query.index.common.doubleval;

import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class DoubleGtOperationSearch extends DoubleEqOperationSearch {

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberIndexFile<Double> snappyIndexFile) {
        Double searchValue = queryValueRetriever.getValue();
        return searchValue < snappyIndexFile.getFrom() || searchValue < snappyIndexFile.getTo();
    }

    @Override
    public boolean matching(Double currentValue, QueryValueRetriever queryValueRetriever) {
        Double searchValue = queryValueRetriever.getValue();
        return currentValue > searchValue;
    }
}