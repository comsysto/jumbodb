package org.jumbodb.database.service.query.index.common.floatval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.numeric.NumberLtOperationSearch;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class FloatLtOperationSearch extends NumberLtOperationSearch<Float, Float, NumberIndexFile<Float>> {

    @Override
    public boolean matching(Float currentValue, QueryValueRetriever queryValueRetriever) {
        Float searchValue = queryValueRetriever.getValue();
        return currentValue < searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Float> snappyIndexFile) {
        Float searchValue = queryValueRetriever.getValue();
        return searchValue > snappyIndexFile.getTo() || searchValue > snappyIndexFile.getFrom();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new FloatQueryValueRetriever(indexQuery);
    }
}
