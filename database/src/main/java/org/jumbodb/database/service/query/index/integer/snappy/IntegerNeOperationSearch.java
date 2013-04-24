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
public class IntegerNeOperationSearch extends NumberNeOperationSearch<Integer, NumberSnappyIndexFile<Integer>> {

    public IntegerNeOperationSearch(NumberSnappyIndexStrategy<Integer, NumberSnappyIndexFile<Integer>> strategy) {
        super(strategy);
    }

    @Override
    public boolean ne(Integer val1, Integer val2) {
        return val1 != val2;
    }

    @Override
    public boolean acceptIndexFile(QueryClause queryClause, NumberSnappyIndexFile<Integer> hashCodeSnappyIndexFile) {
        int searchValue = (Integer)queryClause.getValue();
        return searchValue != hashCodeSnappyIndexFile.getFrom() || searchValue != hashCodeSnappyIndexFile.getTo();
    }

     @Override
    public boolean matching(Integer currentValue, QueryClause queryClause) {
        int searchValue = (Integer)queryClause.getValue();
        return currentValue != searchValue;
    }
}
