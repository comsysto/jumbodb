package org.jumbodb.database.service.query.index.longval.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberLtOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class LongLtOperationSearch extends NumberLtOperationSearch<Long, NumberSnappyIndexFile<Long>> {
    public LongLtOperationSearch(NumberSnappyIndexStrategy<Long, NumberSnappyIndexFile<Long>> strategy) {
        super(strategy);
    }

    @Override
    public boolean matching(Long currentValue, QueryValueRetriever queryValueRetriever) {
        Integer searchValue = queryValueRetriever.getValue();
        return currentValue < searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Long> snappyIndexFile) {
        Integer searchValue = queryValueRetriever.getValue();
        return searchValue < snappyIndexFile.getTo();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new LongQueryValueRetriever(queryClause);
    }
}
