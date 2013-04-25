package org.jumbodb.database.service.query.index.longval.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberEqOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class LongEqOperationSearch extends NumberEqOperationSearch<Long, Long, Long, NumberSnappyIndexFile<Long>> {


    public LongEqOperationSearch(NumberSnappyIndexStrategy<Long, Long, NumberSnappyIndexFile<Long>> strategy) {
        super(strategy);
    }

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
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Long> snappyIndexFile) {
        Long searchValue = queryValueRetriever.getValue();
        return searchValue >= snappyIndexFile.getFrom() && searchValue <= snappyIndexFile.getTo();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new LongQueryValueRetriever(queryClause);
    }
}
