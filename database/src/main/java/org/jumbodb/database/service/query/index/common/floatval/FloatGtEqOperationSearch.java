package org.jumbodb.database.service.query.index.common.floatval;

import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;

/**
 * @author Carsten Hufe
 */
public class FloatGtEqOperationSearch extends FloatEqOperationSearch {

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberIndexFile<Float> snappyIndexFile) {
        Float searchValue = queryValueRetriever.getValue();
        return searchValue <= snappyIndexFile.getFrom() || searchValue <= snappyIndexFile.getTo();
    }

    @Override
    public boolean matching(Float currentValue, QueryValueRetriever queryValueRetriever) {
        Float searchValue = queryValueRetriever.getValue();
        return currentValue >= searchValue;
    }
}
