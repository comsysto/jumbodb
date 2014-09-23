package org.jumbodb.database.service.query.index.common.integer;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.numeric.NumberEqOperationSearch;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class IntegerEqOperationSearch extends NumberEqOperationSearch<Integer, Integer, Integer, NumberIndexFile<Integer>> {
    @Override
    public boolean matching(Integer currentValue, QueryValueRetriever queryValueRetriever) {
        Integer searchValue = queryValueRetriever.getValue();
        return currentValue.equals(searchValue);
    }

    @Override
    public boolean eq(Integer val1, Integer val2) {
        return val1.equals(val2);
    }

    @Override
    public boolean lt(Integer val1, Integer val2) {
        return val1 < val2;
    }

    @Override
    public boolean gt(Integer val1, Integer val2) {
        return val1 > val2;
    }

    @Override
    public boolean ltEq(Integer val1, Integer val2) {
        return val1 <= val2;
    }

    @Override
    public boolean gtEq(Integer val1, Integer val2) {
        return val1 >= val2;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Integer> indexFile) {
        Integer searchValue = queryValueRetriever.getValue();
        return searchValue >= indexFile.getFrom() && searchValue <= indexFile.getTo();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new IntegerQueryValueRetriever(indexQuery);
    }
}
