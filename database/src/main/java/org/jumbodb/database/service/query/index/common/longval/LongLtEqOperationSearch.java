package org.jumbodb.database.service.query.index.common.longval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.numeric.NumberLtOperationSearch;

/**
 * @author Carsten Hufe
 */
public class LongLtEqOperationSearch extends NumberLtOperationSearch<Long, Long, NumberIndexFile<Long>> {

    @Override
    public boolean matching(Long currentValue, QueryValueRetriever queryValueRetriever) {
        Long searchValue = queryValueRetriever.getValue();
        return currentValue <= searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Long> snappyIndexFile) {
        Long searchValue = queryValueRetriever.getValue();
        return searchValue >= snappyIndexFile.getTo() || searchValue >= snappyIndexFile.getFrom();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new LongQueryValueRetriever(indexQuery);
    }
}
