package org.jumbodb.database.service.query.index.common.integer;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.numeric.NumberLtOperationSearch;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class IntegerLtOperationSearch extends NumberLtOperationSearch<Integer, Integer, NumberIndexFile<Integer>> {

    @Override
    public boolean matching(Integer currentValue, QueryValueRetriever queryValueRetriever) {
        Integer searchValue = queryValueRetriever.getValue();
        return currentValue < searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Integer> snappyIndexFile) {
        Integer searchValue = queryValueRetriever.getValue();
        return searchValue > snappyIndexFile.getTo() || searchValue > snappyIndexFile.getFrom();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new IntegerQueryValueRetriever(indexQuery);
    }
}
