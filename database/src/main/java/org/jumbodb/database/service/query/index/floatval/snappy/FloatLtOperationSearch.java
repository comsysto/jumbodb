package org.jumbodb.database.service.query.index.floatval.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberLtOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class FloatLtOperationSearch extends NumberLtOperationSearch<Float, Float, NumberSnappyIndexFile<Float>> {

    @Override
    public boolean matching(Float currentValue, QueryValueRetriever queryValueRetriever) {
        Float searchValue = queryValueRetriever.getValue();
        return currentValue < searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Float> snappyIndexFile) {
        Float searchValue = queryValueRetriever.getValue();
        return searchValue > snappyIndexFile.getTo() || searchValue > snappyIndexFile.getFrom();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new FloatQueryValueRetriever(indexQuery);
    }
}
