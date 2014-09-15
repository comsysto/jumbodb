package org.jumbodb.database.service.query.index.common.longval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.numeric.NumberEqOperationSearch;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class LongEqOperationSearch extends NumberEqOperationSearch<Long, Long, Long, NumberIndexFile<Long>> {

    @Override
    public boolean matching(Long currentValue, QueryValueRetriever queryValueRetriever) {
        Long searchValue = queryValueRetriever.getValue();
        return currentValue.equals(searchValue);
    }

    @Override
    public boolean eq(Long val1, Long val2) {
        return val1.equals(val2);
    }

    @Override
    public boolean lt(Long val1, Long val2) {
        return val1 < val2;
    }

    @Override
    public boolean gt(Long val1, Long val2) {
        return val1 > val2;
    }

    @Override
    public boolean ltEq(Long val1, Long val2) {
        return val1 <= val2;
    }

    @Override
    public boolean gtEq(Long val1, Long val2) {
        return val1 >= val2;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Long> snappyIndexFile) {
        Long searchValue = queryValueRetriever.getValue();
        return searchValue >= snappyIndexFile.getFrom() && searchValue <= snappyIndexFile.getTo();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new LongQueryValueRetriever(indexQuery);
    }
}
