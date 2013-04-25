package org.jumbodb.database.service.query.index.floatval.snappy;

import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class FloatGtOperationSearch extends FloatEqOperationSearch {
    public FloatGtOperationSearch(NumberSnappyIndexStrategy<Float, Float, NumberSnappyIndexFile<Float>> strategy) {
        super(strategy);
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Float> snappyIndexFile) {
        Float searchValue = queryValueRetriever.getValue();
        return searchValue > snappyIndexFile.getFrom();
    }

    @Override
    public boolean matching(Float currentValue, QueryValueRetriever queryValueRetriever) {
        Float searchValue = queryValueRetriever.getValue();
        return currentValue > searchValue;
    }
}
