package org.jumbodb.database.service.query.index.longval.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.basic.numeric.NumberLtOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class LongLtOperationSearch extends NumberLtOperationSearch<Long, Long, NumberSnappyIndexFile<Long>> {

    @Override
    public boolean matching(Long currentValue, QueryValueRetriever queryValueRetriever) {
        Long searchValue = queryValueRetriever.getValue();
        return currentValue < searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberSnappyIndexFile<Long> snappyIndexFile) {
        Long searchValue = queryValueRetriever.getValue();
        return searchValue > snappyIndexFile.getTo() || searchValue > snappyIndexFile.getFrom();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new LongQueryValueRetriever(indexQuery);
    }
}
