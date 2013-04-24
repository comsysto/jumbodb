package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberEqOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberGtOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public class IntegerGtOperationSearch extends IntegerEqOperationSearch {
    public IntegerGtOperationSearch(NumberSnappyIndexStrategy<Integer, NumberSnappyIndexFile<Integer>> strategy) {
        super(strategy);
    }

    @Override
    public boolean acceptIndexFile(QueryClause queryClause, NumberSnappyIndexFile<Integer> hashCodeSnappyIndexFile) {
        int searchValue = (Integer)queryClause.getValue();
        return searchValue > hashCodeSnappyIndexFile.getFrom();
    }

    @Override
    public boolean matching(Integer currentValue, QueryClause queryClause) {
        int searchValue = (Integer)queryClause.getValue();
        return currentValue > searchValue;
    }
}
