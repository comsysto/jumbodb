package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberLtOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class IntegerLtOperationSearch extends NumberLtOperationSearch<Integer, Integer, NumberSnappyIndexFile<Integer>> {
    public IntegerLtOperationSearch(NumberSnappyIndexStrategy<Integer, Integer, NumberSnappyIndexFile<Integer>> strategy) {
        super(strategy);
    }

    @Override
    public boolean matching(Integer currentValue, QueryValueRetriever queryValueRetriever) {
        Integer searchValue = queryValueRetriever.getValue();
        return currentValue < searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Integer> snappyIndexFile) {
        Integer searchValue = queryValueRetriever.getValue();
        return searchValue > snappyIndexFile.getTo() || searchValue > snappyIndexFile.getFrom();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new IntegerQueryValueRetriever(queryClause);
    }
}
