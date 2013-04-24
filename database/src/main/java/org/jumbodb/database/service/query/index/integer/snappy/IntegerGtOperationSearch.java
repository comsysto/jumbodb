package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public class IntegerGtOperationSearch extends IntegerEqOperationSearch {

    @Override
    public boolean matching(Integer currentValue, QueryClause queryClause) {
        int searchValue = (Integer)queryClause.getValue();
        return currentValue > searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryClause queryClause, IntegerSnappyIndexFile hashCodeSnappyIndexFile) {
        int searchValue = (Integer)queryClause.getValue();
        return searchValue > hashCodeSnappyIndexFile.getFromInt();
    }
}
