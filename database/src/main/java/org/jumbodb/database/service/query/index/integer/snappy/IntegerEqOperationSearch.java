package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberEqOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.OperationSearch;
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
    public boolean matching(Integer currentValue, QueryClause queryClause) {
        int searchValue = (Integer)queryClause.getValue();
        return currentValue == searchValue;
    }

    @Override
    public boolean eq(Integer val1, Integer val2) {
        return val1 == val2;
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
    public boolean acceptIndexFile(QueryClause queryClause, NumberSnappyIndexFile<Integer> hashCodeSnappyIndexFile) {
        int searchValue = (Integer)queryClause.getValue();
        return searchValue >= hashCodeSnappyIndexFile.getFrom() && searchValue <= hashCodeSnappyIndexFile.getTo();
    }
}
