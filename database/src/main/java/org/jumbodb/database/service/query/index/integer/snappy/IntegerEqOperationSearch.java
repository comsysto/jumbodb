package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.*;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public class IntegerEqOperationSearch extends NumberEqOperationSearch<Integer, NumberSnappyIndexFile<Integer>> {


    public IntegerEqOperationSearch(NumberSnappyIndexStrategy<Integer, NumberSnappyIndexFile<Integer>> strategy) {
        super(strategy);
    }

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
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Integer> snappyIndexFile) {
        Integer searchValue = queryValueRetriever.getValue();
        return searchValue >= snappyIndexFile.getFrom() && searchValue <= snappyIndexFile.getTo();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new IntegerQueryValueRetriever(queryClause);
    }
}
