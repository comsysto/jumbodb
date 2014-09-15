package org.jumbodb.database.service.query.index.common.floatval;

import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class FloatGtOperationSearch extends FloatEqOperationSearch {

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberIndexFile<Float> snappyIndexFile) {
        Float searchValue = queryValueRetriever.getValue();
        return searchValue < snappyIndexFile.getFrom() || searchValue < snappyIndexFile.getTo();
    }

    @Override
    public boolean matching(Float currentValue, QueryValueRetriever queryValueRetriever) {
        Float searchValue = queryValueRetriever.getValue();
        return currentValue > searchValue;
    }
}
